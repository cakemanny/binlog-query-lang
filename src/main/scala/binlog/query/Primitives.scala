package binlog.query

trait Primitives {

  sealed abstract class CompOp
  case object Eq extends CompOp
  case object Neq extends CompOp
  case object Gt extends CompOp
  case object Gte extends CompOp
  case object Lt extends CompOp
  case object Lte extends CompOp

  sealed abstract class NumOp
  case object Plus extends NumOp
  case object Minus extends NumOp
  case object Multiply extends NumOp
  case object Divide extends NumOp

  sealed abstract class Identifier
  case class UnQualIdent(columnName: String) extends Identifier
  case class QualifiedIdent(tableName: String, columnName: String) extends Identifier
  case class ColumnOrdinal(ord: Long) extends Identifier
  case class QualifiedOrd(tableName: String, ord: Long) extends Identifier

  def colName(ident: Identifier): String = ident match {
    case UnQualIdent(name) => name
    case QualifiedIdent(_, name) => name
    case ColumnOrdinal(ord) => ord.toString
    case QualifiedOrd(_, ord) => ord.toString
  }

  // unboxing constructors
  def ColumnOrdinal(lit: LongL): ColumnOrdinal =
    ColumnOrdinal(lit.l)
  def QualifiedOrd(col: String, lit: LongL): QualifiedOrd =
    QualifiedOrd(col, lit.l)

  sealed abstract class Literal
  case class LongL(l: Long) extends Literal
  case class StrL(str: String) extends Literal
  case object NullL extends Literal

  // A few of common shared literals
  val LongL0: LongL = LongL(0L)
  val LongL1: LongL = LongL(1L)
  val StrL0: StrL = StrL("0")
  val StrL1: StrL = StrL("1")
  val StrLEmpty: StrL = StrL("")

  sealed abstract class ScalarFunction
  case object Concat extends ScalarFunction
  case object RegExp extends ScalarFunction
  case object Lower extends ScalarFunction
  case object Upper extends ScalarFunction
  case object Length extends ScalarFunction
  case object Substr extends ScalarFunction

  sealed abstract class AggregateFunction
  case object Avg extends AggregateFunction
  case object Count extends AggregateFunction
  case object Max extends AggregateFunction
  case object Min extends AggregateFunction
  case object Sum extends AggregateFunction

}

