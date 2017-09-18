package binlog.query

object Main {

  def runQueryAndPrint(query: String): Unit = {
    runQuery(query){ header =>
      println(header mkString "\t")
    }{ row =>
      import QueryAST._
      val forDisplay = row.map{
        case StrL(s) => s
        case LongL(l) => l.toString
        case DoubleL(d) => d.toString
        case NullL => "NULL"
      }
      println(forDisplay mkString "\t")
    }
  }

  def raise(msg: String): Nothing = {
    throw new ClientErrorException(msg)
  }

  def runQuery(query: String)
              (header: Vector[String] => Unit)
              (yld: Vector[QueryAST.Literal] => Unit): Unit = {
    val parsed = BQLParser.parseBql(query)

    // 1. collate binlog references

    import QueryAST._
    import matryoshka._
    import matryoshka.implicits._
    import matryoshka.data._
    import scalaz._, Scalaz._

    println(parsed)

    def validateFile(filePath: String): ValidationNel[String, String] = {
      import java.nio.file._
      val path = Paths.get(filePath)
      try { // just prove we can open for reading
        val inputStream = Files.newInputStream(path)
        inputStream.close()
        filePath.successNel
      } catch {
        case t: Throwable =>
          (t.getClass.getSimpleName.toString + ": " + t.getMessage).failureNel
      }
    }

    def validateConnectionString(ds: String): ValidationNel[String, String] = {
      // want mysql://user:pass@localhost:3306/
      // TODO: allow specifying the binlog file at the end
      val pat = raw"mysql://(?:(\w+)[:](\w+)@)?([A-Za-z0-9.]+)(?:[:]([0-9]+))?/?".r
      ds match {
        case pat(user, pass, hostname, port) =>
          println("host:port: $hostname:$port")
          ds.successNel
        case _ =>
          ("connection string must " +
            "match \"mysql://[{user}:{password}@]{hostname}[:{port}]\" format").failureNel
      }
      // are we meant to try connect at this point?
    }

    val checkedDsValid: ValidationNel[String, String] = parsed match {
      case Fix(Select(_, ds, _, _, _)) =>
         validateConnectionString(ds) orElse validateFile(ds)
      case Fix(Stream(_, ds, _, _, _)) =>
        validateConnectionString(ds)
      case _ => sys.error("not allowed by grammar")
    }


    import scala.util.Try

    def castToLong(lit: Literal): LongL = lit match {
      case l@LongL(_) => l
      case StrL(s) => Try(s.toLong).fold(_ => LongL0, LongL)
      case NullL => LongL0
      case DoubleL(d) => LongL(d.toLong)
    }
    def castToStr(lit: Literal): StrL = lit match {
      case s@StrL(_) => s
      case LongL(l) => l match {
        case 0 => StrL0
        case 1 => StrL1
        case l => StrL(l.toString)
      }
      case NullL => StrLEmpty
      case DoubleL(d) => StrL(d.toString)
    }
    @inline def bool2Long(b: Boolean): LongL = if (b) LongL1 else LongL0

    def any2colType(colType: => DataAccess.ColType)(value: Any): Literal = {
      import DataAccess._
      colType match {
        case LongCol => LongL(value match {
          case i: java.lang.Integer => i.toLong
          case l: java.lang.Long => l.toLong
          case s: String => s.toLong
          case v => v.asInstanceOf[Long]
        })
        case StringCol => StrL(value match {
          case d: java.sql.Timestamp => d.toString
          case d: java.sql.Date => d.toString
          case d: java.util.Date => java.sql.Timestamp.from(d.toInstant).toString
          case otherwise => otherwise.toString
        })
        case BlobCol => StrL(value match {
          case s: String => s
          case bs: Array[Byte] => try {
            new String(bs, 0, bs.size)
          } catch {
            case _: Exception => "0x" + bs.map("%02X" format _).mkString
          }
        })
        case DoubleCol => DoubleL(value match {
          case d: java.lang.Double => d.toDouble
          case f: java.lang.Float => f.toDouble
          case s: String => s.toDouble
          case v => v.asInstanceOf[Double]
        })
      }
    }

    @inline def minL(lhs: LongL, rhs: LongL): LongL =
      if (lhs.l < rhs.l) lhs else rhs
    @inline def maxL(lhs: LongL, rhs: LongL): LongL =
      if (lhs.l > rhs.l) lhs else rhs
    // This exhibits a problem, we don't get a shortcutting AND or OR...
    // maybe we need a different kind of fold than cata...
    def evalExpr(evt: DataAccess.Event): Algebra[ExprF, Literal] = {
      case Select(_, _, _, _, _) => sys.error("nested selects prohibited")
      case Stream(_, _, _, _, _) => sys.error("nested stream")
      case And(exprs) =>
        exprs.foldLeft(LongL1){ (mem, expr) => minL(mem, castToLong(expr)) }
      case Or(exprs) =>
        exprs.foldLeft(LongL0){ (mem, expr) => maxL(mem, castToLong(expr)) }
      case Not(expr) =>
        if (castToLong(expr) == LongL0) LongL1 else LongL0
      case Comp(compOp, lhs, rhs) =>
        // if either side is null, then Null
        // if either side is LongL then cast both sides
        // otherwise do string comparison
        if (lhs == NullL || rhs == NullL) NullL
        else (lhs, rhs) match {
          case (StrL(slhs), StrL(srhs)) =>
            @inline def appStr(f: (String, String) => Boolean) =
              bool2Long(f(slhs, srhs))
            compOp match {
              case Eq => appStr(_ == _)
              case Neq => appStr(_ != _)
              case Gt => appStr(_ > _)
              case Gte => appStr(_ >= _)
              case Lt => appStr(_ < _)
              case Lte => appStr(_ <= _)
            }
          case _ =>
            @inline def appLong(f: (Long, Long) => Boolean) =
              bool2Long(f(castToLong(lhs).l, castToLong(rhs).l))
            compOp match {
            case Eq => appLong(_ == _)
            case Neq => appLong(_ != _)
            case Gt => appLong(_ > _)
            case Gte => appLong(_ >= _)
            case Lt => appLong(_ < _)
            case Lte => appLong(_ <= _)
          }
        }
      case Like(expr, pattern) =>
        val str = castToStr(expr).str
        val asRegex = pattern.flatMap{c => if (c == '%') ".*" else c.toString}
        bool2Long(str.matches(asRegex))
      case BinOp(numOp, lhs, rhs) =>
        val f: (Long, Long) => Long = numOp match {
          case QueryAST.Plus => { _ + _ }
          case QueryAST.Minus => { _ - _ }
          case QueryAST.Multiply => { _ * _ }
          case QueryAST.Divide => { _ / _ }
        }
        Try(f(castToLong(lhs).l, castToLong(rhs).l)).fold(_ => NullL, LongL)
      case Lit(lit) => lit
      case Ident(ident) => {// TODO: resolve reference against row
        // we should probably start by trying to convert Query into ROW for
        // complete update or insert statements
        import DataAccess._
        // ident ^ tableInfo => colIndex
        // data ^ colIndex => value
        ident match {
          case QualifiedIdent(tableName, columnName) =>
            (tableName, columnName, evt) match {
              case ("meta", "table_name", Row(tableInfo, _, _, _)) =>
                StrL(tableInfo.tableName)
              case ("meta", "table_name", Query(_,_)) => NullL
              case ("meta", "table_schema", Row(tableInfo, _, _, _)) =>
                StrL(tableInfo.database)
              case ("meta", "table_schema", Query(_,_)) => NullL
              case ("meta", "query", Query(sql, _)) => StrL(sql)
              case ("meta", "query", Row(_, _, _, _)) => NullL
              case ("meta", "position", evt) => LongL(evt.meta.position)
              case ("meta", "timestamp", evt) =>
                StrL(new java.sql.Timestamp(evt.meta.timestamp).toString)
              case ("meta", "server_id", evt) => LongL(evt.meta.serverId)
              case ("meta", "xid", evt) => LongL(evt.meta.xid)
              case ("meta", unknownCol, _) =>
                raise(s"unknown col meta.$unknownCol")
              case _ => ??? // TODO: new, old, data
            }
          case UnQualIdent(columnName) =>
            evt match {
              // read some caches columnName => index map
              case Row(_,_,_,_) => NullL // TODO
              case Query(sql, _) =>
                import InsertUpdateAST._
                Try(DMLParser.parseDML(sql)) match {
                  case scala.util.Success(Insert(tbl, Some(cols), vals)) =>
                    ??? // index of columnName in cols, lookup in vals
                  case scala.util.Success(Update(tbl, vals, pred)) =>
                    ??? // _2 where _1 of val in vals == columnName
                  case _ => ???
                }
            }
          case ColumnOrdinal(ord) => evt match {
            case Row(tableInfo, _, data, _) =>
              val value = data(ord.toInt)
              any2colType(tableInfo.schema(ord.toInt))(value)
            case Query(sql, _) => ??? // parse SQL?
          }
          case QualifiedOrd(tableName, ord) => evt match {
            case Row(tableInfo, before, data, _) =>
              val image =
                if (Seq(tableInfo.tableName,"data","new") contains tableName)
                  Some(data)
                else if (tableName == "old") before
                else None

              image.filter(ord.toInt < _.size)
                .flatMap(data => Option(data(ord.toInt)))
                .map(any2colType(tableInfo.schema(ord.toInt)))
                .getOrElse(NullL)

            case Query(sql, _) =>
              // TODO: parse SQL?
              NullL
          }
        }
      }
      case FnCall(fnName, args) =>
        // ideally #arg checking can be done at
        // planning time rather execution time
        def checkArgs(numExpected: Int)(lit: => Literal): Literal =
          if (args.size == numExpected) lit
          else raise("incorrect number of args to function " + fnName)

        fnName match {
          case "concat" => StrL(args.map(s => castToStr(s).str).mkString)
          case "regexp" => checkArgs(2){
            bool2Long(castToStr(args(0)).str.matches(castToStr(args(1)).str))
          }
          case "lower" => checkArgs(1){
            StrL(castToStr(args.head).str.toLowerCase)
          }
          case "upper" => checkArgs(1){
            StrL(castToStr(args.head).str.toUpperCase)
          }
          case "length" => checkArgs(1){
            LongL(castToStr(args.head).str.length.toLong)
          }
          case "substr" =>
            if (args.size == 2) {
              val str = castToStr(args(0)).str
              val pos = castToLong(args(1)).l.toInt
              StrL(str.substring(Math.min(pos, str.length)))
            } else if (args.size == 3) {
              val str = castToStr(args(0)).str
              val pos = castToLong(args(1)).l.toInt
              val len = castToLong(args(2)).l.toInt
              StrL(str.substring(Math.min(pos, str.length), Math.min(pos + len, str.length)))
            } else raise("incorrect number of args to function " + fnName)
          case _ =>
            raise("unknown function: " + fnName)
        }
      case Case(scrutinee, branches, elseValue) => // TODO
        branches.view
          .filter(p => scrutinee == p._1)
          .map(_._2)
          .headOption.getOrElse(elseValue)
    }

    def evalFilter(flt: Fix[ExprF])(evt: DataAccess.Event): Boolean = flt.unFix match {
      case Select(_, _, _, _, _) => sys.error("nested select")
      case Stream(_, _, _, _, _) => sys.error("nested stream")
      case And(exprs) =>
        exprs.forall(expr => evalFilter(expr)(evt))
      case Or(exprs) =>
        exprs.foldLeft(false) { (soFar, expr) => soFar || evalFilter(expr)(evt) }
      case Not(expr) =>
        !evalFilter(expr)(evt)
      case Comp(_, _, _) => flt.cata(evalExpr(evt)) == LongL1
      case Like(expr, pattern) =>
        val matcher = expr.cata(evalExpr(evt))
        val asRegex = pattern.flatMap{c => if (c == '%') ".*" else c.toString}
        castToStr(matcher).str.matches(asRegex)
      case BinOp(_, _, _) =>
        castToLong(flt.cata(evalExpr(evt))).l != 0
      case Lit(lit) => lit match {
        case LongL(l) => l != 0
        case StrL(str) => str != null && str != ""
        case NullL => false
        case DoubleL(d) => d != 0.0
      }
      case Ident(ident) =>
        // devolve to literal case
        evalFilter(Fix[ExprF](Lit(flt.cata(evalExpr(evt)))))(evt)
      case FnCall(_, _) =>
        evalFilter(Fix[ExprF](Lit(flt.cata(evalExpr(evt)))))(evt)
      case Case(scrutinee, branches, elseValue) => // TODO
        // Something like first branch where Comp(Eq,scrut, _1) == true give _2
        // TODO: evaluate scrutinee at most once
        evalFilter(
          branches.view
            .filter(p => evalFilter(Fix(Comp(Eq, scrutinee, p._1)))(evt))
            .map(_._2)
            .headOption.getOrElse(elseValue)
        )(evt)
    }

    // Evaluate
    def execQuery(op: Fix[ExprF]): Unit = op match {
      case Fix(Select(prj, ds, flt, grp, lim)) =>
        // * We want to use parts of the filter to influence how we scan the file!
        // Not possible with shyiko apparently!
        // * Want to reorder Ands and Ors from least computational to most
        // * Want to fold constant expressions
        val theFilter = flt.getOrElse(Fix[ExprF](Lit(LongL1)))

        val startTime = System.currentTimeMillis
        var rowsReturned = 0L

        // print header
        header(prj._1)
        // Want to branch strategy for group by case
        // FIX ME, if it's a mysql://... we want to stream somewhat
        DataAccess.scanFile(ds) { evt =>
          // evaluate the filter against the event
          if (evalFilter(theFilter)(evt)) {
            if (lim.map(_ > rowsReturned).getOrElse(true)) {
              rowsReturned += 1L
              // project? or group?
              val projected = prj._2.map{ _.cata(evalExpr(evt)) }
              yld(projected)
            }
          }
        }

        val endTime = System.currentTimeMillis
        println(s"# $rowsReturned rows returned in ${endTime - startTime}ms")
      case Fix(Stream(prj, ds, flt, grp, lim)) =>
        val theFilter = flt.getOrElse(Fix[ExprF](Lit(LongL1)))
        var rowsReturned = 0L

        header(prj._1)

        DataAccess.connectAndStream(ds) { evt =>
          // evaluate the filter against the event
          if (evalFilter(theFilter)(evt)) {
            if (lim.map(_ > rowsReturned).getOrElse(true)) {
              rowsReturned += 1L
              // project? or group?
              val projected = prj._2.map{ _.cata(evalExpr(evt)) }
              yld(projected)
            }
          }
        }

      case _ => sys.error("disallowed expression at top level")
    }

    (for {
      _ <- checkedDsValid
    } yield {
      execQuery(parsed)
    }) match {
      case Success(_) => println("success")
      case Failure(msgs) => raise("failure: " + msgs.toList.mkString("; "))
    }
  }

}

