package binlog.query

import utest._
import matryoshka.data.Fix
import QueryAST._
import BQLParser.Grammar.Expr


object ExecutionTest extends TestSuite {

  val tests = Tests {
    "meta.query" - {
      val query =
        """select meta.query from "test_files/mysqld-bin.000004" limit 1"""
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("query"))
      }
      Main.runQuery(query)(header){ row =>
        assert(row == Vector(StrL("create database test")))
      }
    }

    "meta.server_id" - {
      val query =
        """select meta.server_id from "test_files/mysqld-bin.000004" limit 1"""
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("server_id"))
      }
      Main.runQuery(query)(header){ row =>
        assert(row == Vector(LongL(1)))
      }
    }

    "meta" - {
      val query = """
      |  select meta.position, meta.timestamp, meta.table_schema, meta.table_name
      |    from "test_files/mysqld-bin.000004"
      """.stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "timestamp", "table_schema", "table_name"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
          Vector(LongL(120), StrL("2018-09-04 23:46:07.0"), NullL, NullL),
          Vector(LongL(214), StrL("2018-09-04 23:47:07.0"), NullL, NullL),
          Vector(LongL(328), StrL("2018-09-04 23:48:48.0"), NullL, NullL),
          Vector(LongL(450), StrL("2018-09-04 23:48:48.0"), StrL("test"), StrL("test")),
          Vector(LongL(450), StrL("2018-09-04 23:48:48.0"), StrL("test"), StrL("test")),
          Vector(LongL(450), StrL("2018-09-04 23:48:48.0"), StrL("test"), StrL("test")),
          Vector(LongL(450), StrL("2018-09-04 23:48:48.0"), StrL("test"), StrL("test")),
          Vector(LongL(544), StrL("2018-09-04 23:48:57.0"), NullL, NullL),
          Vector(LongL(666), StrL("2018-09-04 23:48:57.0"), StrL("test"), StrL("test")),
          Vector(LongL(666), StrL("2018-09-04 23:48:57.0"), StrL("test"), StrL("test")),
          Vector(LongL(666), StrL("2018-09-04 23:48:57.0"), StrL("test"), StrL("test")),
          Vector(LongL(666), StrL("2018-09-04 23:48:57.0"), StrL("test"), StrL("test")),
          Vector(LongL(789), StrL("2018-09-04 23:49:21.0"), NullL, NullL)
      ))
    }

    "meta: unknown col" - {
      // header still executes
      // maybe it would be better to work out whether the columns are real,
      // before starting execution
      val query =
        """select meta.not_a_real_col from "test_files/mysqld-bin.000004" limit 1"""
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("not_a_real_col"))
      }
      intercept[binlog.query.ClientErrorException] {
        Main.runQuery(query)(header){ row =>
          assert(false)
        }
      }
    }

    "old, new: insert" - {
      val query = """
      | select old.[0], old.[1], new.[0], new.[1]
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 450
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("0", "1", "0", "1"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(NullL, NullL, LongL(1), StrL("a")),
        Vector(NullL, NullL, LongL(2), StrL("b")),
        Vector(NullL, NullL, LongL(3), StrL("c")),
        Vector(NullL, NullL, LongL(4), StrL("d")),
      ))
    }

    "old, new: update" - {
      val query = """
      | select old.[0], old.[1], new.[0], new.[1]
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 666
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("0", "1", "0", "1"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(1), StrL("a"), LongL(2), StrL("a")),
        Vector(LongL(2), StrL("b"), LongL(3), StrL("b")),
        Vector(LongL(3), StrL("c"), LongL(4), StrL("c")),
        Vector(LongL(4), StrL("d"), LongL(5), StrL("d")),
      ))
    }

    "unqualified ord" - {
      val query = """
      | select [0], data.[1]
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 450
      | limit 1
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("0", "1"))
      }
      Main.runQuery(query)(header){ row =>
        assert(row == Vector(LongL(1), StrL("a")))
      }
    }

    "and" - {
      val query = """
      | select [0], data.[1]
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 450 and [0] = 1
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("0", "1"))
      }
      Main.runQuery(query)(header){ row =>
        assert(row == Vector(LongL(1), StrL("a")))
      }
    }

    "or" - {
      val query = """
      | select meta.position, [0], [1]
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 450 or [0] = 2
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "0", "1"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(450), LongL(1), StrL("a")),
        Vector(LongL(450), LongL(2), StrL("b")),
        Vector(LongL(450), LongL(3), StrL("c")),
        Vector(LongL(450), LongL(4), StrL("d")),

        Vector(LongL(666), LongL(2), StrL("a")),
      ))
    }

    "not" - {
      val query = """
      | select meta.position, [0], [1]
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 450 and not [0] = 2
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "0", "1"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(450), LongL(1), StrL("a")),
        Vector(LongL(450), LongL(3), StrL("c")),
        Vector(LongL(450), LongL(4), StrL("d")),
      ))
    }

    "like" - {
      val query = """
      | select meta.position, [0], [1]
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 450 and [1] like 'a%'
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "0", "1"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(450), LongL(1), StrL("a")),
      ))
    }

    "plus" - {
      val query = """
      | select meta.position, old.[0], new.[0], old.[0] + new.[0] as added
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 666
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "0", "0", "added"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(666), LongL(1), LongL(2), LongL(3)),
        Vector(LongL(666), LongL(2), LongL(3), LongL(5)),
        Vector(LongL(666), LongL(3), LongL(4), LongL(7)),
        Vector(LongL(666), LongL(4), LongL(5), LongL(9)),
      ))
    }

    // TODO: continue here with
    // * function calls
    // * case expressions
    //

    "group" - {
      val query = """
      | select meta.position, sum(old.[0]) as total
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 666
      | group by position
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "total"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(666), LongL(10)),
      ))
    }

    "group:count" - {
      val query = """
      | select meta.position, count(old.[0]) as total
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 666
      | group by position
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "total"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(666), LongL(4)),
      ))
    }

    "group:min" - {
      val query = """
      | select meta.position, min(old.[0]) as total
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 666
      | group by position
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "total"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(666), LongL(1)),
      ))
    }

    "group:max" - {
      val query = """
      | select meta.position, max(old.[0]) as total
      | from "test_files/mysqld-bin.000004"
      | where meta.position = 666
      | group by position
      |""".stripMargin
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position", "total"))
      }
      var rows = Vector.empty[Vector[QueryAST.Literal]]
      Main.runQuery(query)(header){ row =>
        rows = rows :+ row
      }
      assert(rows == Vector(
        Vector(LongL(666), LongL(4)),
      ))
    }
  }

}
