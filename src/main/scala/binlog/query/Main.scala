package binlog.query

object Main {

  def runQuery(query: String): Unit = {

    val parsed = BQLParser.parseBql(query)

    // 1. collate binlog references

    import QueryAST._
    import matryoshka._
    import matryoshka.implicits._
    import matryoshka.data._
    type Operator = Fix[OperatorF]

    def getRefs: OperatorF[String] => String = {
      case Scan(filename) => filename
      case Stream(conn) => conn
      case Filter(_, parRef) => parRef
      case Project(_,_, parRef) => parRef
      case Group(_, parRef) => parRef
      case Limit(_, parRef) => parRef
    }

    println(parsed.cata(getRefs))

  }

}

