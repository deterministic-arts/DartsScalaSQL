package darts.lib.sql.jdbc

import java.sql.PreparedStatement
import scala.annotation.{tailrec => loop}

trait Fragment {

    def isEmpty: Boolean

    def text: String

    def substitutions: Seq[Substitution[_]]
}

final object Fragment {

    val Empty: Fragment = TextFrag("")

    def concatenate2(frag1: Fragment, frag2: Fragment): Fragment =
        if (frag1.isEmpty) frag2
        else if (frag2.isEmpty) frag1
        else frag1 match {
            case TextFrag(tx1) => frag2 match {
                case TextFrag(tx2) => TextFrag(tx1 + tx2)
                case _ => SequenceFrag(List(frag1, frag2))
            }
            case SequenceFrag(ls1) => frag2 match {
                case SequenceFrag(ls2) => SequenceFrag(ls1 ::: ls2)
                case _ => SequenceFrag(ls1 ::: List(frag2))
            }
            case _ => frag2 match {
                case SequenceFrag(ls2) => SequenceFrag(frag1 :: ls2)
                case _ => SequenceFrag(List(frag1, frag2))
            }
        }

    def concatenate(frags: Fragment*): Fragment =
        frags.foldRight(Empty)(concatenate2)
}

final class SqlStringContextExtensions(val value: StringContext) extends AnyVal {

    import Fragment._

    def sql(frags: Fragment*): Fragment = {

        val strs = value.parts.toList.reverse
        val args = frags.toList.reverse

        @loop def conc(frag: Fragment, ss: List[String], fs: List[Fragment]): Fragment = {
            if (ss.isEmpty) frag
            else if (ss.head.isEmpty) conc(frag, ss.tail, fs)
            else if (fs.isEmpty) conc(concatenate2(TextFrag(ss.head), frag), ss.tail, fs)
            else conc(concatenate2(TextFrag(ss.head), concatenate2(fs.head, frag)), ss.tail, fs.tail)
        }

        conc(TextFrag(strs.head), strs.tail, args)
    }
}

trait FragmentImplicits {

    implicit def sqlFragmentStringContextExtensions(context: StringContext): SqlStringContextExtensions =
        new SqlStringContextExtensions(context)
}

final case class TextFrag(val text: String) extends Fragment {
    def substitutions: Seq[Substitution[_]] = List()

    def isEmpty: Boolean = text.isEmpty
}

final case class SequenceFrag(val fragments: List[Fragment]) extends Fragment {
    def text: String = fragments.map(_.text).mkString("", "", "")

    def isEmpty: Boolean = fragments.forall(_.isEmpty)

    def substitutions: Seq[Substitution[_]] = fragments.flatMap(_.substitutions)
}

final case class Slot[T](val name: String, val descriptor: Type[T])
        extends Substitution[T]
        with Fragment {

    def isEmpty: Boolean = false
    def text: String = "?"
    def substitutions: Seq[Substitution[_]] = List(this)

    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver) {
        descriptor.bindValue(stmt, index, bindings(this))
    }

    def bind(value: Option[T]): Bindings.Binding[T] = Bindings.Binding(this, value)

    def apply(value: T): Bindings.Binding[T] = bind(Some(value))

    lazy val empty: Bindings.Binding[T] = bind(None)

    override def toString: String = "Slot[" + descriptor + "](" + name + ")"
}

final case class Constant[T](val value: Option[T], val descriptor: Type[T])
        extends Substitution[T]
        with Fragment {

    def isEmpty: Boolean = false
    def text: String = "?"
    def substitutions: Seq[Substitution[_]] = List(this)

    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver) {
        descriptor.bindValue(stmt, index, value)
    }

    override def toString: String = "Constant[" + descriptor + "](" + value + ")"
}
