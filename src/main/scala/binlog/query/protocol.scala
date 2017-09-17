package binlog.query

// Various different flags and constants

object Capabilities {
  val CLIENT_LONG_PASSWORD = 0x0001
  val CLIENT_FOUND_ROWS = 0x0002
  val CLIENT_LONG_FLAG = 0x0004
  val CLIENT_CONNECT_WITH_DB = 0x0008
  val CLIENT_NO_SCHEMA = 0x0010
  val CLIENT_COMPRESS = 0x0020
  val CLIENT_ODBC = 0x0040
  val CLIENT_LOCAL_FILES = 0x0080
  val CLIENT_IGNORE_SPACE = 0x0100
  val CLIENT_PROTOCOL_41 = 0x0200
  val CLIENT_INTERACTIVE = 0x0400
  val CLIENT_SSL = 0x0800
  val CLIENT_IGNORE_SIGPIPE = 0x1000
  val CLIENT_TRANSACTIONS = 0x2000
  val CLIENT_RESERVED = 0x4000
  val CLIENT_SECURE_CONNECTION = 0x8000
  val CLIENT_MULTI_STATEMENTS = 0x10000
  val CLIENT_MULTI_RESULTS = 0x20000
  val CLIENT_REMEMBER_OPTIONS = 0x80000000
}

object CharacterCodes {
  // Don't really care about any others
  val utf8_general_ci = 33
}

object StatusFlags {
  val SERVER_STATUS_AUTOCOMMIT = 2
}

object CommandType {
  val COM_QUIT = 1
  val COM_INIT_DB = 2
  val COM_QUERY = 3
  val COM_REFRESH = 7
  val COM_SHUTDOWN = 8
  val COM_PING = 14
  val COM_BINLOG_DUMP = 18
  val COM_PREPARE = 22
  val COM_EXECUTE = 23
  val COM_LONG_DATA = 24
  val COM_CLOSE_STMT = 25
  val COM_RESET_STMT = 26
}

object FieldType {
  val MYSQL_TYPE_DECIMAL = 0
  val MYSQL_TYPE_TINY = 1
  val MYSQL_TYPE_SHORT = 2
  val MYSQL_TYPE_LONG = 3
  val MYSQL_TYPE_FLOAT = 4
  val MYSQL_TYPE_DOUBLE = 5
  val MYSQL_TYPE_NULL = 6
  val MYSQL_TYPE_TIMESTAMP = 7
  val MYSQL_TYPE_LONGLONG = 8
  val MYSQL_TYPE_INT24 = 9
  val MYSQL_TYPE_DATE = 10
  val MYSQL_TYPE_TIME = 11
  val MYSQL_TYPE_DATETIME = 12
  val MYSQL_TYPE_YEAR = 13
  val MYSQL_TYPE_NEWDATE = 14
  val MYSQL_TYPE_VARCHAR = 15
  val MYSQL_TYPE_BIT = 16
  val MYSQL_TYPE_TIMESTAMP2 = 17
  val MYSQL_TYPE_DATETIME2 = 18
  val MYSQL_TYPE_TIME2 = 19
  val MYSQL_TYPE_NEWDECIMAL = 246
  val MYSQL_TYPE_ENUM = 247
  val MYSQL_TYPE_SET = 248
  val MYSQL_TYPE_TINY_BLOB = 249
  val MYSQL_TYPE_MEDIUM_BLOB = 250
  val MYSQL_TYPE_LONG_BLOB = 251
  val MYSQL_TYPE_BLOB = 252
  val MYSQL_TYPE_VAR_STRING = 253
  val MYSQL_TYPE_STRING = 254
  val MYSQL_TYPE_GEOMETRY = 255
}

