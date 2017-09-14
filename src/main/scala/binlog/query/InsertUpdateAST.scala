package binlog.query

object InsertUpdateAST extends Primitives {

  // Allow a parse of a very limited subset of insert and update statements

  sealed abstract class Command

  case class Update(
    tableName: String,
    values: Seq[(String, Literal)],
    predicate: Seq[Pred] // Combined with AND
  ) extends Command

  case class Insert(
    tableName: String,
    columnList: Option[Seq[String]],
    valueList: Seq[Literal]
  ) extends Command

  sealed abstract class Pred
  case class EqP(lhs: String, rhs: Literal) extends Pred
  case class GtP(lhs: String, rhs: Literal) extends Pred
  case class GteP(lhs: String, rhs: Literal) extends Pred
  case class LtP(lhs: String, rhs: Literal) extends Pred
  case class LteP(lhs: String, rhs: Literal) extends Pred


}

