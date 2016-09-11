package darts.lib.sql.jdbc

import java.sql.PreparedStatement

sealed trait Fragment {

    def ~:(frag: Fragment): Fragment

    def isEmpty: Boolean

    def text: String

    def substitutions: Seq[Substitution[_]]

    def toTemplate: Template =
        new Template(text, substitutions)
}

final object Fragment {

    def slot[T](name: String)(implicit descriptor: Type[T]): Slot[T] =
        Slot(name, descriptor)

    def constant[T](value: T)(implicit descriptor: Type[T]): Constant[T] =
        Constant(Some(value), descriptor)

    def missing[T](implicit descriptor: Type[T]): Constant[T] =
        Constant(None, descriptor)

    implicit def fragmentFromString(text: String): Fragment =
        TextFrag(text)

    implicit def fragmentFromSubstitution(subst: Substitution[_]): Fragment =
        SubstitutionFrag(subst)

    private val Comma = TextFrag(", ")

    def listOf(frags: Fragment*): Fragment =
        frags.foldRight(EmptyFrag: Fragment) { (elt, accu) => if (accu == EmptyFrag) elt else elt ~: Comma ~: accu }

    def listOfConstants[T](values: T*)(implicit descriptor: Type[T]): Fragment =
        listOf(values.map(p => SubstitutionFrag(constant(p))): _*)

    implicit final class SqlStringContextExtensions(val value: StringContext) extends AnyVal {

        def sql(frags: Fragment*): Fragment = {

            val strs = value.parts.toList.reverse
            val args = frags.toList.reverse

            @scala.annotation.tailrec def loop(frag: Fragment, ss: List[String], fs: List[Fragment]): Fragment = {
                if (ss.isEmpty) frag
                else if (fs.isEmpty) loop(ss.head ~: frag, ss.tail, fs)
                else loop(ss.head ~: fs.head ~: frag, ss.tail, fs.tail)
            }

            loop(strs.head, strs.tail, args)
        }
    }

}

final case object EmptyFrag extends Fragment {

    def ~:(frag: Fragment): Fragment = frag

    def isEmpty: Boolean = true

    def text: String = ""

    def substitutions: Seq[Substitution[_]] = List()
}

final case class TextFrag(val text: String) extends Fragment {
    def substitutions: Seq[Substitution[_]] = List()

    def isEmpty: Boolean = text.isEmpty

    def ~:(frag: Fragment): Fragment = frag match {
        case TextFrag(more) => TextFrag(more + text)
        case _ => SequenceFrag(frag :: List(this))
    }
}

final case class SubstitutionFrag(val substitution: Substitution[_]) extends Fragment {
    def ~:(frag: Fragment): Fragment = SequenceFrag(frag :: List(this))

    def isEmpty: Boolean = false

    def text: String = "?"

    def substitutions: Seq[Substitution[_]] = List(substitution)
}

final case class SequenceFrag(val fragments: List[Fragment]) extends Fragment {
    def text: String = fragments.map(_.text).mkString("", "", "")

    def isEmpty: Boolean = fragments.forall(_.isEmpty)

    def substitutions: Seq[Substitution[_]] = fragments.flatMap(_.substitutions)

    def ~:(frag: Fragment): Fragment = frag match {
        case SequenceFrag(head) => SequenceFrag(head ::: fragments)
        case _ => SequenceFrag(frag :: fragments)
    }
}

final case class Slot[T](val name: String, val descriptor: Type[T])
    extends Substitution[T] {

    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver) {
        descriptor.bindValue(stmt, index, bindings(this))
    }

    def bind(value: Option[T]): Bindings.Binding[T] = Bindings.Binding(this, value)

    def apply(value: T): Bindings.Binding[T] = bind(Some(value))

    lazy val empty: Bindings.Binding[T] = bind(None)

    override def toString: String = "Slot[" + descriptor + "](" + name + ")"
}

final case class Constant[T](val value: Option[T], val descriptor: Type[T])
    extends Substitution[T] {
    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver) {
        descriptor.bindValue(stmt, index, value)
    }

    override def toString: String = "Constant[" + descriptor + "](" + value + ")"
}
