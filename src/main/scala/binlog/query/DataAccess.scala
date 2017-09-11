package binlog.query

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
import com.github.shyiko.mysql.binlog.event.{EventType, TableMapEventData, UpdateRowsEventData, WriteRowsEventData}

// abstract over the binlogc-connector library
object DataAccess {


  sealed abstract class ColType
  final case object LongCol extends ColType
  final case object StringCol extends ColType
  final case object BlobCol extends ColType

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
      val colTypes = tableMapEntry.getColumnTypes

      val schema: IndexedSeq[ColType] = colTypes.map(ColumnType.byCode(_)) map {
        case ColumnType.DECIMAL => LongCol
        case ColumnType.TINY => LongCol
        case ColumnType.SHORT => LongCol
        case ColumnType.LONG => LongCol
        case ColumnType.FLOAT => LongCol // we will just discard floating point!
        case ColumnType.DOUBLE => LongCol
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
        case ColumnType.NEWDECIMAL => LongCol
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
  def mkInsertRow(tableInfo: TableInfo, data: WriteRowsEventData, meta: MetaInfo): Seq[Row] = {
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
  def scanFile(path: String)(yld: Event => Unit): Unit = {
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

          case EventType.TABLE_MAP =>
            val data = event.getData[bl.event.TableMapEventData]
            val tableInfo = TableInfo(data)
            // update internal table map
            tableMap.get(data.getTableId) match  {
              case Some(found) if found == tableInfo => ()
              case _ => tableMap = tableMap + (data.getTableId -> tableInfo)
            }
          case EventType.EXT_WRITE_ROWS =>
            val data = event.getData[bl.event.WriteRowsEventData]
            // maybe think about producing a lazy data structure...?

            tableMap.get(data.getTableId) match {
              case Some(tableInfo) =>
                mkInsertRow(tableInfo, data, meta) foreach yld
              case None => () // log warning?
            }
          case EventType.EXT_UPDATE_ROWS =>
            val data = event.getData[bl.event.UpdateRowsEventData]
            tableMap.get(data.getTableId) match {
              case Some(tableInfo) =>
                mkUpdateRows(tableInfo, data, meta) foreach yld
              case None => () // log warning?
            }

          case EventType.EXT_DELETE_ROWS =>
            val data = event.getData[bl.event.DeleteRowsEventData]

          case _ => ()
        }

        event = binlogReader.readEvent()
      }
    } finally {
      binlogReader.close()
    }
  }

}

