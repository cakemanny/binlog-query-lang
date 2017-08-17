package binlog.query

object QueryAST extends Primitives {

  sealed abstract class ExprF[A]
  case class Select[A](
    projection: (Vector[String], Vector[A]),
    dataSource: String, // filename or connection string
    filterPred: Option[A],
    groupingKeys: Option[Vector[String]],
    limit: Option[Long]
  ) extends ExprF[A]
  case class Stream[A](
    projection: (Vector[String], Vector[A]),
    connectionString: String, // connection string
    filterPred: Option[A],
    groupingKeys: Option[Vector[String]],
    limit: Option[Long]
  ) extends ExprF[A]
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

  // define a functor instance to allow matryoshka to work

  implicit val exprFunctor: scalaz.Functor[ExprF] = new scalaz.Functor[ExprF] {
    import scalaz.syntax.functor._, scalaz.std.tuple._
    def map[A, B](fa: ExprF[A])(f: A => B): ExprF[B] = fa match {
      case Select(prj, ds, fp, gk, l) =>
        Select(prj.map(_ map f), ds, fp map f, gk, l)
      case Stream(prj, ds, fp, gk, l) =>
        Stream(prj.map(_ map f), ds, fp map f, gk, l)
      case And(exprs) => And(exprs.map(f))
      case Or(exprs) => Or(exprs.map(f))
      case Not(expr) => Not(f(expr))
      case Comp(op, lhs, rhs) => Comp(op, f(lhs), f(rhs))
      case Like(expr, pattern) => Like(f(expr), pattern)
      case BinOp(op, lhs, rhs) => BinOp(op, f(lhs), f(rhs))
      case Lit(lit) => Lit(lit)
      case Ident(ident) => Ident(ident)
      case FnCall(fnName, args) => FnCall(fnName, args.map(f))
      case Case(scrutinee, branches, elseValue) =>
        Case(f(scrutinee), branches.map{case (a1,a2) => (f(a1),f(a2))}, f(elseValue))
    }
  }


}


