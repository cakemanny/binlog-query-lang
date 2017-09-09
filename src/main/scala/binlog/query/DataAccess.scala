package binlog.query

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
import com.github.shyiko.mysql.binlog.event.{EventType, TableMapEventData, UpdateRowsEventData, WriteRowsEventData}

// abstract over the binlogc-connector library
object DataAccess {


  sealed abstract class ColType
  final case object LongCol extends ColType
  final case object StringCol extends ColType
  final case object BlobCol extends ColType

  sealed abstract class Event
  // we keep schema separate so that we can reuse it
  final case class Row(
    tableInfo: TableInfo,
    before: Option[IndexedSeq[Any]],
    data: IndexedSeq[Any]
  ) extends Event

  // we ought to want to parse this for value only insert/update
  final case class Query(sql: String) extends Event


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

  def mkUpdateRows(tableInfo: TableInfo, data: UpdateRowsEventData): Seq[Row] = {
    import scala.collection.JavaConverters.asScalaBuffer
    asScalaBuffer(data.getRows).map { mapEntry =>
      val beforeImage = mapEntry.getKey
      val afterImage = mapEntry.getValue
      Row(
        tableInfo,
        Some(beforeImage.toIndexedSeq),
        afterImage.toIndexedSeq
      )
    }
  }
  def mkInsertRow(tableInfo: TableInfo, data: WriteRowsEventData): Seq[Row] = {
    import scala.collection.JavaConverters.asScalaBuffer
    asScalaBuffer(data.getRows).map { Row(tableInfo, None, _) }
  }

  // should scan file take a continuation or produce an iterator/stream...
  // let's try continuation
  def scanFile(path: String)(yld: Event => Unit): Unit = {
    import com.github.shyiko.mysql.{ binlog => bl }
    import bl.BinaryLogFileReader
    import bl.event.EventHeader

    val file = new java.io.File(path)
    val binlogReader = new BinaryLogFileReader(file)
    try {

      var tableMap = Map.empty[Long,TableInfo]

      var event = binlogReader.readEvent()
      while (event != null) {

        // want to turn shyiko event into our event type
        val eventType = event.getHeader[EventHeader].getEventType

        eventType match {
          case EventType.QUERY =>
            val queryEventData = event.getData[bl.event.QueryEventData]
            yld(Query(queryEventData.getSql))

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
                mkInsertRow(tableInfo, data) foreach yld
              case None => () // log warning?
            }
          case EventType.EXT_UPDATE_ROWS =>
            val data = event.getData[bl.event.UpdateRowsEventData]
            tableMap.get(data.getTableId) match {
              case Some(tableInfo) =>
                mkUpdateRows(tableInfo, data) foreach yld
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

