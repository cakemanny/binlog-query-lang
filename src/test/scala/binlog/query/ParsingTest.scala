package binlog.query

import utest._
import matryoshka.data.Fix
import QueryAST._
import BQLParser.Grammar.Expr

object ParsingTest extends TestSuite {

  def canParse(query: String): Unit = {
    BQLParser.parseBql(query)
  }
  def cantParse(query: String): Unit = {
    intercept[Exception] { BQLParser.parseBql(query) }
  }

  // Just some quickfire sanity, does it parse tests
  val tests = Tests {
    // prove our subsequent tests will be working
    "Throws Exception when parsing fails" - {
      intercept[Exception] { canParse("bad query") }
    }
    "select"        - { canParse("""select x from "t" """) }
    "stream"        - { canParse("""stream x from "t" """) }
    "where"         - { canParse("""select x from "t" where 1 """) }
    "group by"      - { canParse("""select x from "t" group by x """) }
    "group multi"   - { canParse("""select x, y from "t" group by x, y """) }
    "limit"         - { canParse("""select x from "t" limit 1 """) }
    "limit 0"       - { canParse("""select x from "t" limit 0 """) }
    "limit pos"     - {cantParse("""select x from "t" limit -1 """) }
    // field List
    "alias"         - { canParse("""select 1 as x from "t" """) }
    "qualified"     - { canParse("""select x.y from "t" """) }
    "expressions require alias" - {
                       cantParse("""select 1 from "t" """)
    }
    // comments
    "comments *"    - { canParse("""select /* 'file */ f from "t" """) }
    "comments #"    - { canParse("""select f # file
                                    from "t" """) }
    "comments --"   - { canParse("""select f -- 'file
                                    from "t" """) }
    // literals
    "numbers"       - { canParse("""select 1 as x from "t" """) }
    "numbers"       - { canParse("""select 12345 as x from "t" """) }
    "numbers neg"   - { canParse("""select -1 as x from "t" """) }
    "numbers no oct"- {cantParse("""select 0123 as x from "t" """) }
    "strings"       - { canParse("""select 'x' as x from "t" """) }
    "ordinals"      - { canParse("""select [0] as x from "t" """) }
    "ordinals"      - { canParse("""select [123456789] as x from "t" """) }
    "ordinals pos"  - {cantParse("""select [-1] as x from "t" """) }
    "ordinals qual" - { canParse("""select d.[0] as x from "t" """) }

    // Expressions
    "plus"          - { canParse("""select 1 + 1 as x from "t" """) }
    "plus"          - { canParse("""select 1+1+1 as x from "t" """) }
    "plus no dbl"   - {cantParse("""select 1 + +1 as x from "t" """) }
    "minus"         - { canParse("""select 1 - 1 as x from "t" """) }
    "minus"         - { canParse("""select 1-1 as x from "t" """) }
    "minus"         - { canParse("""select 1 - 1 - 1 as x from "t" """) }
    "plus add neg"  - { canParse("""select 1 + -1 as x from "t" """) }
    "minus no dbl"  - {cantParse("""select 1 - - 1 as x from "t" """) }
    "minus sub neg" - { canParse("""select 1 - -1 as x from "t" """) }
    "times"         - { canParse("""select 1 * 1 as x from "t" """) }
    "times"         - { canParse("""select 1*1*1 as x from "t" """) }
    "divide"        - { canParse("""select 1 / 1 as x from "t" """) }
    "divide"        - { canParse("""select 1/1/1 as x from "t" """) }
    "compare ="     - { canParse("""select 1 = 1 as x from "t" """) }
    "compare !="    - { canParse("""select 1 != 1 as x from "t" """) }
    "compare >"     - { canParse("""select 1 > 1 as x from "t" """) }
    "compare <"     - { canParse("""select 1 < 1 as x from "t" """) }
    "compare <="    - { canParse("""select 1 <= 1 as x from "t" """) }
    "compare >="    - { canParse("""select 1 >= 1 as x from "t" """) }
    "not"           - { canParse("""select not 1 as f from "t" """) }
    "and"           - { canParse("""select 1 and 1 as f from "t" """) }
    "and and"       - { canParse("""select 1 and 1 and 1 as f from "t" """) }
    "or"            - { canParse("""select 0 or 1 as f from "t" """) }
    "or or"         - { canParse("""select 0 or 1 or 2 as f from "t" """) }
    "like"          - { canParse("""select 1 like 'x' as f from "t" """) }
    // Should this test be here? Maybe we want to add in future?
    "like lit-only" - {cantParse("""select 1 like concat(x,y) as f from "t" """) }
    "not like"      - { canParse("""select 1 not like 'x' as f from "t" """) }
    "functions"     - { canParse("""select f(x) as y from "t" """) }
    "func no qual"  - {cantParse("""select fs.f(x) as y from "t" """) }
    "fun multi"     - { canParse("""select f(x, y) as z from "t" """) }
    "case"          - { canParse("""select case x when 1 then 2 end as x from "t" """) }
    "case multi"    - { canParse("""select case x when 1 then 2 when 2 then 3 end as x from "t" """) }
    "case else"     - { canParse("""select case x when 1 then 2 else 3 end as x from "t" """) }
    "case else mult"- { canParse("""select case x when 1 then 2 when 2 then 3 else 4end as x from "t" """) }

    import BQLParser.{ parseBql => parse }
    // Check tree order for - and / make sense
    "divide order" - {
      val tree = parse("""select 1 / 2 / 3 as x from "t"  """)
      val expected = parse("""select (1 / 2) / 3 as x from "t" """)
      val notExpected = parse("""select 1 / (2 / 3) as x from "t" """)
      assert(tree == expected, tree != notExpected)
    }
    "and precedence.left" - {
      val tree = parse("""select 1 and 2 or 3 as x from "t" """)
      val expected = parse("""select (1 and 2) or 3 as x from "t" """)
      val notExpected = parse("""select 1 and (2 or 3) as x from "t" """)
      assert(tree == expected, tree != notExpected)
    }
    "and precedence.right" - {
      val tree = parse("""select 1 or 2 and 3 as x from "t" """)
      val expected = parse("""select 1 or (2 and 3) as x from "t" """)
      val notExpected = parse("""select (1 or 2) and 3 as x from "t" """)
      assert(tree == expected, tree != notExpected)
    }
    "* precedence.left" - {
      val tree = parse("""select 1 * 2 + 3 as x from "t" """)
      val expected = parse("""select (1 * 2) + 3 as x from "t" """)
      val notExpected = parse("""select 1 * (2 + 3) as x from "t" """)
      assert(tree == expected, tree != notExpected)
    }
    "* precedence.right" - {
      val tree = parse("""select 1 + 2 * 3 as x from "t" """)
      val expected = parse("""select 1 + (2 * 3) as x from "t" """)
      val notExpected = parse("""select (1 + 2) * 3 as x from "t" """)
      assert(tree == expected, tree != notExpected)
    }
  }
}

