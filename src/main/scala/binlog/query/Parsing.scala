package binlog.query

/**
 * Informal Grammar (an almost subset of mysql select grammar)
 *
 * query: select_query | explain_stmt
 *
 * explain_stmt: EXPLAIN select_query
 *
 * select_query:
 *   { SELECT | STREAM }
 *      select_expr, [, select_expr ... ]
 *   FROM binlog_reference
 *   [ WHERE where_condition ]
 *   [ GROUP BY colname, ... ]
 *   [ LIMIT row_count ]
 *
 * select_expr:
 *   expr [ AS identifier ]
 * where_condition:
 *   expr
 *
 * expr:
 *   expr AND expr
 * | expr OR expr
 * | NOT expr
 * | expr comparison_operator expr
 * | expr [ NOT ] LIKE string_literal
 * | bit_expr
 *
 * comparison_operator: = | != | &gt; | &lt; | &gt;= | &lt;=
 *
 * bit_expr:
 *   bit_expr '+' bit_expr
 * | bit_expr '-' bit_expr
 * | bit_expr '*' bit_expr
 * | bit_expr '/' bit_expr
 * | simple_expr
 *
 * simple_expr:
 *   literal
 * | identifier
 * | function_call
 * | case_expr
 *
 */
object BQLParser {

  def parseBql(input: String): matryoshka.data.Fix[QueryAST.ExprF] =
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
      "select", "stream", "from", "where", "group", "by", "as", "limit",
      "not", "like", "null", "explain", "case", "then", "else", "end",
      "and", "or"
    )
    val reservedList = Vector(
      "inner", "outer", "left", "right", "natural", "join"
    )

    val ident = P(
      (letter | "_") ~ (letter | digit | "_").rep
    ).!.filter(!(keywordList ++ reservedList).contains(_))

    import QueryAST.{ QualifiedIdent, UnQualIdent, ColumnOrdinal, QualifiedOrd }
    import QueryAST.{ LongL, StrL, NullL }

    val qualifiedIdent = P( ident ~ "." ~ ident ).map{
      case (tbl, col) => QualifiedIdent(tbl, col)
    }

    val unqualified = P( ident ).map(UnQualIdent)

    val ordinal = P( "[" ~ nonNegative ~ "]" ).map(ColumnOrdinal)

    val qualifiedOrdinal = P( ident ~ ".[" ~ nonNegative ~ "]").map{
      case (tbl, ord) => QualifiedOrd(tbl, ord)
    }

    // TODO: escape sequences!

    val stringLiteral: Parser[StrL] =
      P( "'" ~/ CharsWhile(_ != '\'', min=0).! ~ "'").map(StrL)

    val nonNegative: Parser[LongL] =
      P( nonZerodigit ~ digit.rep | "0" ).!.map(s => LongL(s.toLong))

    val intLiteral: Parser[LongL] =
      P( "-".? ~ nonZerodigit ~ digit.rep | "0" ).!.map(s => LongL(s.toLong))

    val quoted: Parser[String] =
      P( "\"" ~/ CharsWhile(_ != '"', min=0).! ~ "\"")

  }

  object Grammar {
    val WsApi = fastparse.WhitespaceApi.Wrapper {
      import fastparse.all._
      NoTrace((
        CharIn(" \t\n\r")
        | ("#" | "-- ") ~/ CharsWhile(_ != '\n', min=0)
        | "/*" ~/ (CharsWhile(_ != '*') | "*" ~ !"/").rep ~ "*/"
      ).rep)
    }
    import fastparse.noApi._
    import WsApi._
    import Lexical._
    import QueryAST._
    import matryoshka._
    import matryoshka.implicits._
    import matryoshka.data._

    type Expr = Fix[ExprF]

    def parseToEnd(input: String) = (stmt ~ End).parse(input)
    def parseAll(input: String): Expr = parseToEnd(input) match {
      case Parsed.Success(res, _) => res
      case res => throw new Exception(res.toString)
    }

    /** Grammar starting point */
    val stmt = P( selectQuery | kw("explain") ~ selectQuery )

    val selectQuery: Parser[Expr] =
      P( selectClause ~ fromClause ~ whereClause.? ~ groupClause.? ~ limitClause.? ).map{
        case (t, p, s, f, g, l) => Fix(t(p, s, f, g, l): ExprF[Expr])
      }

    val selectClause = P( selectOrStream ~/ fieldList )
    val selectOrStream =
      P( kw("select").map(_ => Select[Expr] _)
        | kw("stream").map(_ => Stream[Expr] _) )

    val fromClause: Parser[String] = P( kw("from") ~/ quoted )

    val whereClause: Parser[Expr] = P( kw("where") ~/ expr )

    val groupClause: Parser[Vector[String]] =
      P( kw("group") ~ kw("by") ~/ idList )

    val limitClause: Parser[Long] = P( kw("limit") ~/ nonNegative ).map{ _.l }

    val idList = P( ident.rep(min=1,sep=",".~/) ).map(_.toVector)

    val fieldList: Parser[(Vector[String], Vector[Expr])] =
      P( ( expr ~ kw("as") ~ ident
         | identifier.map( ide => (Fix(Ident(ide)): Expr, colName(ide)) )
         ).rep(min=1,sep=",")
       ).map(_.toVector.unzip.swap)

    val expr: Parser[Expr] = // disjunction
      P( conjunction.rep(min=1, sep=kw("or")) ).map{
        xs => if (xs.size > 1) Fix(Or(xs.toVector)) else xs.head
      }
    val conjunction: Parser[Expr] =
      P( (negation | comparison).rep(min=1, sep=kw("and")) ).map{
        xs => if (xs.size > 1) Fix(And(xs.toVector)) else xs.head
      }
    val negation: Parser[Expr] =
      P( kw("not") ~ comparison ).map(e => Fix(Not(e)))
    val comparison: Parser[Expr] =
      P( bitExpr ~ (
          ( compOp ~ bitExpr ).map{ case (op, rhs) => (bit: Expr) => Fix(Comp(op, bit, rhs)) }
        | ( kw("like") ~/ stringLiteral ).map{ lit => (bit: Expr) => Fix(Like(bit, lit.str))  }
        | ( kw("not") ~ kw("like") ~/ stringLiteral ).map{ lit => (bit: Expr) => Fix(Not(Fix(Like(bit, lit.str))))  }
      ).? ).map{
        case (bit, Some(f)) => f(bit)
        case (bit, None) => bit
      }
    val compOp: Parser[CompOp] =
      P(  P("=").map(_ => Eq)
        | P("!=").map(_ => Neq)
        | P("<>").map(_ => Neq)
        | P("<=").map(_ => Lte)
        | P("<").map(_ => Lt)
        | P(">=").map(_ => Gte)
        | P(">").map(_ => Gte)
      )

    def liftOp(op: NumOp) = (a: Expr, b: Expr) => Fix(BinOp(op, a, b))

    val plusOrMinus =
      P(  P("+").map(_ => liftOp(Plus))
        | P("-").map(_ => liftOp(Minus))
      )

    val bitExpr: Parser[Expr] = {
      val rightOp =
        P(plusOrMinus ~ product).map{ case (op, e2) => op(_: Expr, e2) }
      P(
        product ~ rightOp.rep.map(Function.chain)
      ).map{case (e1, rhs) => rhs(e1)}
    }

    //val bitExpr: Parser[Expr] =
    //  P( product.rep(min=1, sep="+") ).map{ _.reduceLeft((e1,e2) => Fix(BinOp(Plus, e1, e2))) }

    val starOrSlash: Parser[(Expr,Expr) => Expr] =
      P(  P("*").map(_ => liftOp(Multiply))
        | P("/").map(_ => liftOp(Divide))
      )

    // a ~ [(* b) (* c)] =>  a ~ x => ((x * b) * c)  => ((a * b) * c)
    val product: Parser[Expr] = {
      val rightOp =
        P(starOrSlash ~ simpleExpr).map{ case (op, e2) => op(_: Expr, e2) }
      P(
        simpleExpr ~ rightOp.rep.map(Function.chain)
      ).map{case (e1, rhs) => rhs(e1)}
    }

    val simpleExpr: Parser[Expr] =
      P( literal
        | functionCall
        | identifier.map(i => Fix(Ident(i)): Expr)
        | caseExpr
        | subExpr
        )

    val literal: Parser[Expr] =
      P( stringLiteral | intLiteral | kw("null").map(_ => NullL) ).map{
        lit => Fix(Lit(lit))
      }

    val functionCall: Parser[Expr] =
      P( ident ~ "(" ~/ expr.rep(sep=",") ~ ")" ).map{
        case (fnName, args) => Fix(FnCall(fnName, args))
      }

    val identifier: Parser[Identifier] =
      P( qualifiedOrdinal | ordinal | qualifiedIdent | unqualified )

    val caseExpr: Parser[Expr] =
      P( kw("case") ~/ expr ~ whenClauses ~ (kw("else") ~ expr).? ~ kw("end")).map{
        case (scrut, br, elseV) =>
          Fix(Case(scrut, br, elseV.getOrElse(Fix(Lit(NullL)))))
      }
    val whenClauses =
      P( kw("when") ~/ expr ~ kw("then") ~/ expr ).rep(min=1).map(_.toVector)

    val subExpr: Parser[Expr] = P( "(" ~/ expr ~ ")" )

  }

}
