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
      def yld(row: Vector[QueryAST.Literal]): Unit = {
        assert(row == Vector(StrL("create database test")))
      }
      Main.runQuery(query)(header)(yld)
    }

    "meta.position" - {
      val query =
        """select meta.position from "test_files/mysqld-bin.000004" limit 1"""
      def header(cols: Vector[String]): Unit = {
        assert(cols == Vector("position"))
      }
      def yld(row: Vector[QueryAST.Literal]): Unit = {
        assert(row == Vector(LongL(120)))
      }
      Main.runQuery(query)(header)(yld)
    }
  }

}
