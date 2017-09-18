package binlog.query

object DMLParser {

  def parseDML(input: String): InsertUpdateAST.Command =
    Grammar.parseAll(input.trim)

  object Lexical {
    import fastparse.all._

    def kw(s: String) = IgnoreCase(s) ~ !(letter | digit | "_")
    val letter        = P( lowercase | uppercase )
    val lowercase     = P( CharIn('a' to 'z') )
    val uppercase     = P( CharIn('A' to 'Z') )
    val digit         = P( CharIn('0' to '9') )
    val nonZerodigit  = P( CharIn('1' to '9') )

    val keywordList = Vector(
      "insert", "ignore", "into", "values", "where",
      "update", "set", "null", "and"
    )
    val reservedList = Vector(
      "inner", "outer", "left", "right", "natural", "join", "or"
    )

    val ident = P(
      (letter | "_") ~ (letter | digit | "_").rep
    ).!.filter(!(keywordList ++ reservedList).contains(_))

    import InsertUpdateAST.{ LongL, StrL, DoubleL, NullL }

    // TODO: escape sequences
    val stringLiteral: Parser[StrL] =
      P( "'" ~/ CharsWhile(_ != '\'', min=0).! ~ "'").map(StrL)

    val intLiteral: Parser[LongL] =
      P( nonZerodigit ~ digit.rep | "0" ).!.map(s => LongL(s.toLong))

    // TODO: doubles and decimals
    // TODO: base64 and base64 wrapped blobs

    val quoted: Parser[String] =
      P( "\"" ~/ CharsWhile(_ != '"', min=0).! ~ "\"")

    val backQuoted: Parser[String] =
      P( "`" ~/ CharsWhile(_ != '`', min=0).! ~ "`")

  }

  object Grammar {
    val WsApi = fastparse.WhitespaceApi.Wrapper {
      import fastparse.all._
      NoTrace((" " | "\t" | "\n").rep)
    }
    import fastparse.noApi._
    import WsApi._
    import Lexical._
    import InsertUpdateAST._

    def parseToEnd(input: String) = (stmt ~ End).parse(input)
    def parseAll(input: String): Command = parseToEnd(input) match {
      case Parsed.Success(res, _) => res
      case res => throw new Exception(res.toString)
    }

    /** Grammar starting point */

    val stmt: Parser[Command] = P( insertStmt | updateStmt )

    val insertStmt: Parser[Command] =
      P( kw("insert") ~/ kw("ignore").? ~ kw("into").?
        ~ tableName ~ columnList.? ~ kw("values") ~ valueList
      ).map{
          case (tbl, cols, values) => Insert(tbl, cols, values)
      }

    val updateStmt: Parser[Command] =
      P( kw("update") ~/ tableName ~ kw("set") ~ colValueAssignments
        ~ kw("where") ~ predicates
      ).map{
        case (tbl, values, predicate) => Update(tbl, values, predicate)
      }

    // Could be qualified?
    val tableName: Parser[String] = P( quoted | backQuoted | ident )

    val columnName: Parser[String] = P( quoted | backQuoted | ident )

    val columnList: Parser[Seq[String]] =
      P( "(" ~ columnName.rep(sep=",",min=1) ~ ")" )

    val valueList: Parser[Seq[Literal]] =
      P( "(" ~ literal.rep(sep=",", min=1) ~ ")")

    val literal: Parser[Literal] =
      P( stringLiteral | intLiteral | kw("null").map(_ => NullL) )

    val colValueAssignments: Parser[Seq[(String, Literal)]] =
      P( (columnName ~ "=" ~ literal).rep(sep=",", min=1) )

    val predicates: Parser[Seq[Pred]] = P( pred.rep(sep=kw("and")) )

    val pred: Parser[Pred] =
      P(
        (columnName ~ "=" ~ literal).map{case (c,l) => EqP(c,l)}
      | (columnName ~ ">" ~ literal).map{case (c,l) => GtP(c,l)}
      | (columnName ~ ">=" ~ literal).map{case (c,l) => GteP(c,l)}
      | (columnName ~ "<" ~ literal).map{case (c,l) => LtP(c,l)}
      | (columnName ~ "<=" ~ literal).map{case (c,l) => LteP(c,l)}
      )

  }

}

