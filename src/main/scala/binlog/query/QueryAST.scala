package binlog.query

object QueryAST extends Primitives {

  sealed abstract class OperatorF[A]
  case class Scan[A](filename: String) extends OperatorF[A]
  case class Stream[A](connection: String) extends OperatorF[A]
  case class Filter[A,E](pred: E, parent: A) extends OperatorF[A]
  case class Project[A,E](
    colNames: Vector[String], colExprs: Vector[E], parent: A
  ) extends OperatorF[A]
  case class Group[A](keys: Vector[String], parent: A) extends OperatorF[A]
  case class Limit[A](rowCount: Long, parent: A) extends OperatorF[A]

  sealed abstract class ExprF[A]
  case class And[A](exprs: Seq[A]) extends ExprF[A]
  case class Or[A](exprs: Seq[A]) extends ExprF[A]
  case class Not[A](expr: A) extends ExprF[A]
  case class Comp[A](op: CompOp, lhs: A, rhs: A) extends ExprF[A]
  case class Like[A](expr: A, pattern: String) extends ExprF[A]
  case class BinOp[A](op: NumOp, lhs: A, rhs: A) extends ExprF[A]
  case class Lit[A](lit: Literal) extends ExprF[A]
  case class Ident[A](ident: Identifier) extends ExprF[A]
  case class FnCall[A](fnName: String, args: Seq[A]) extends ExprF[A]
  case class Case[A](scrutinee: A, branches: Seq[(A, A)], elseValue: A) extends ExprF[A]

  // define a function instance to allow matryoshka to work

  implicit val opFunctor = new scalaz.Functor[OperatorF] {
    def map[A, B](fa: OperatorF[A])(f: A => B): OperatorF[B] = fa match {
      case Scan(s) => Scan(s)
      case Stream(s) => Stream(s)
      case Filter(e, a) => Filter(e, f(a))
      case Project(x0,x1, a) => Project(x0,x1, f(a))
      case Group(x, a) => Group(x, f(a))
      case Limit(x, a) => Limit(x, f(a))
    }
  }


}


