package binlog.query

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
import com.github.shyiko.mysql.binlog.event.{EventType, TableMapEventData, UpdateRowsEventData, WriteRowsEventData}

// abstract over the binlog-connector library
object DataAccess {

  val ConnectionStringPattern =
    raw"mysql://(?:(?<username>\w+)[:](?<password>[^@]+)@)?(?<hostname>[^:\s]+)(?:[:](?<port>[0-9]+))?(?:/(?<logname>.+)?)?".r

  // A slightly nicer looking way to display the string format
  val connectionStringMessage =
    "mysql://[{user}:{password}@]{hostname}[:{port}][/{logname}]"

  sealed abstract class ColType
  final case object LongCol extends ColType
  final case object StringCol extends ColType
  final case object BlobCol extends ColType
  final case object DoubleCol extends ColType

  sealed abstract class Event {
    def meta: MetaInfo
  }
  // we keep schema separate so that we can reuse it
  final case class Row(
    tableInfo: TableInfo,
    before: Option[IndexedSeq[Any]],
    data: IndexedSeq[Any],
    meta: MetaInfo
  ) extends Event

  // we ought to want to parse this for value only insert/update
  final case class Query(sql: String, meta: MetaInfo) extends Event

  case class MetaInfo(
    position: Long,
    serverId: Long,
    timestamp: Long,
    xid: Long
  )

  case class TableInfo(database: String, tableName: String, schema: IndexedSeq[ColType])

  object TableInfo {
    def apply(tableMapEntry: TableMapEventData): TableInfo = {
      import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
      val colTypes = tableMapEntry.getColumnTypes.map(java.lang.Byte.toUnsignedInt)

      val schema: IndexedSeq[ColType] = colTypes.map(ColumnType.byCode(_)) map {
        case ColumnType.DECIMAL => DoubleCol
        case ColumnType.TINY => LongCol
        case ColumnType.SHORT => LongCol
        case ColumnType.LONG => LongCol
        case ColumnType.FLOAT => DoubleCol // we will just discard floating point!
        case ColumnType.DOUBLE => DoubleCol
        case ColumnType.NULL => StringCol
        case ColumnType.TIMESTAMP => StringCol
        case ColumnType.LONGLONG => LongCol
        case ColumnType.INT24 => LongCol
        case ColumnType.DATE => StringCol
        case ColumnType.TIME => StringCol
        case ColumnType.DATETIME => StringCol
        case ColumnType.YEAR => LongCol
        case ColumnType.NEWDATE => StringCol
        case ColumnType.VARCHAR => StringCol
        case ColumnType.BIT => LongCol
        case ColumnType.TIMESTAMP_V2 => StringCol
        case ColumnType.DATETIME_V2 => StringCol
        case ColumnType.TIME_V2 => StringCol
        case ColumnType.JSON => StringCol
        case ColumnType.NEWDECIMAL => DoubleCol
        case ColumnType.ENUM => StringCol
        case ColumnType.SET => StringCol
        case ColumnType.TINY_BLOB => BlobCol
        case ColumnType.MEDIUM_BLOB => BlobCol
        case ColumnType.LONG_BLOB => BlobCol
        case ColumnType.BLOB => BlobCol
        case ColumnType.VAR_STRING => StringCol
        case ColumnType.STRING => StringCol
        case ColumnType.GEOMETRY => BlobCol
      }

      TableInfo(tableMapEntry.getDatabase, tableMapEntry.getTable, schema)
    }
  }

  def expandIncludedCols(image: Array[java.io.Serializable], included: java.util.BitSet): IndexedSeq[Any] = {
    var i = 0
    var j = 0
    val buf = scala.collection.mutable.ArrayBuffer.empty[Any]
    while (i < image.size) {
      if (included.get(j)) {
        buf += image(i)
        i += 1
      } else {
        buf += null
      }
      j += 1
    }
    buf.toVector
  }

  def mkUpdateRows(tableInfo: TableInfo, data: UpdateRowsEventData, meta: MetaInfo): Seq[Row] = {
    import scala.collection.JavaConverters.asScalaBuffer
    asScalaBuffer(data.getRows).map { mapEntry =>
      val beforeImage = expandIncludedCols(mapEntry.getKey, data.getIncludedColumnsBeforeUpdate)
      val afterImage = expandIncludedCols(mapEntry.getValue, data.getIncludedColumns)
      Row(
        tableInfo,
        Some(beforeImage),
        afterImage,
        meta
      )
    }
  }
  def mkInsertRows(tableInfo: TableInfo, data: WriteRowsEventData, meta: MetaInfo): Seq[Row] = {
    import scala.collection.JavaConverters.asScalaBuffer
    asScalaBuffer(data.getRows).map { values =>
      Row(
        tableInfo,
        None,
        expandIncludedCols(values, data.getIncludedColumns),
        meta
      )
    }
  }

  // should scan file take a continuation or produce an iterator/stream...
  // let's try continuation
  def scanFile(path: String, breakWhen: ((Long,Long)) => Boolean)
              (yld: Event => Unit)
      : Unit = {
    import com.github.shyiko.mysql.{ binlog => bl }
    import bl.BinaryLogFileReader
    import bl.event.{ EventHeader, EventHeaderV4 }
    import bl.event.deserialization.{ EventDeserializer, ChecksumType }

    val file = new java.io.File(path)
    val deserializer = new EventDeserializer()
    deserializer.setChecksumType(ChecksumType.CRC32)
    val binlogReader = new BinaryLogFileReader(file, deserializer)
    try {

      var tableMap = Map.empty[Long,TableInfo]

      var event = binlogReader.readEvent()
      while (event != null) {
        val header = event.getHeader[EventHeaderV4]
        if (breakWhen((header.getPosition, header.getTimestamp))) {
          event = null
        } else {
          tableMap = processSingleEvent(event, tableMap)(yld)
          event = binlogReader.readEvent()
        }
      }
    } finally {
      binlogReader.close()
    }
  }

  import com.github.shyiko.mysql.binlog.event.{ Event => BLEvent }

  // Shared for both scanning and streaming
  def processSingleEvent(event: BLEvent, tableMap: Map[Long, TableInfo])
                        (yld: Event => Unit)
      : Map[Long,TableInfo] = {
    import com.github.shyiko.mysql.{ binlog => bl }
    import bl.event.{ EventHeader, EventHeaderV4 }
    import bl.event.deserialization.{ EventDeserializer, ChecksumType }

    // want to turn shyiko event into our event type
    val eventType = event.getHeader[EventHeader].getEventType

    val header = event.getHeader[EventHeaderV4]
    val meta = MetaInfo(
      header.getPosition,
      header.getServerId,
      header.getTimestamp,
      xid = 0L // we could wait until the transaction completes to
      // produce the event...
      // todo: buffer the events and add xid when we know it
    )

    eventType match {
      case EventType.QUERY =>
        val queryEventData = event.getData[bl.event.QueryEventData]
        yld(Query(queryEventData.getSql, meta))
        tableMap
      case EventType.TABLE_MAP =>
        val data = event.getData[bl.event.TableMapEventData]
        val tableInfo = TableInfo(data)
        // update internal table map
        tableMap.get(data.getTableId) match  {
          case Some(found) if found == tableInfo => tableMap
          case _ => tableMap + (data.getTableId -> tableInfo)
        }
      case EventType.EXT_WRITE_ROWS =>
        val data = event.getData[bl.event.WriteRowsEventData]
        // maybe think about producing a lazy data structure...?

        tableMap.get(data.getTableId) match {
          case Some(tableInfo) =>
            mkInsertRows(tableInfo, data, meta) foreach yld
          case None => () // log warning?
        }
        tableMap
      case EventType.EXT_UPDATE_ROWS =>
        val data = event.getData[bl.event.UpdateRowsEventData]
        tableMap.get(data.getTableId) match {
          case Some(tableInfo) =>
            mkUpdateRows(tableInfo, data, meta) foreach yld
          case None => () // log warning?
        }
        tableMap
      case EventType.EXT_DELETE_ROWS =>
        val data = event.getData[bl.event.DeleteRowsEventData]
        // todo: DeleteRows
        tableMap
      case _ => tableMap
    }
  }

  def connectAndStream(connectionString: String)(yld: Event => Unit): Unit = {
    val (user, pass, hostname, port, logname) = connectionString match {
      case ConnectionStringPattern(user, pass, hostname, port, logname) =>
        (
          Option(user).getOrElse(""),
          Option(pass).getOrElse(""),
          hostname,
          Option(port).map(_.toInt).getOrElse(3306),
          Option(logname)
        )
    }

    import com.github.shyiko.mysql.{ binlog => bl }
    import bl.BinaryLogClient
    import bl.BinaryLogClient.EventListener

    var tableMap = Map.empty[Long,TableInfo]

    val client = new BinaryLogClient(hostname, port, user, pass)
    client.registerEventListener(new EventListener {
      override def onEvent(event: bl.event.Event): Unit = {
        tableMap = processSingleEvent(event, tableMap)(yld)
      }
    })
    client.connect()

  }

}

