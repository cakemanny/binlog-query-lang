package binlog.query

object Main {

  def runQuery(query: String): Unit = {

    val parsed = BQLParser.parseBql(query)

    // 1. collate binlog references

    import QueryAST._
    import matryoshka._
    import matryoshka.implicits._
    import matryoshka.data._

    println(parsed)

  }

}

