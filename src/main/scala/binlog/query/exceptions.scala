package binlog.query

class BinlogQueryException(msg: String) extends Exception(msg)

class ClientErrorException(msg: String) extends BinlogQueryException(msg)

