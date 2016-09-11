package darts.lib.sql.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

sealed trait Fragment {

    def ~:(frag: Fragment): Fragment

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

    def text: String = ""

    def substitutions: Seq[Substitution[_]] = List()
}

final case class TextFrag(val text: String) extends Fragment {
    def substitutions: Seq[Substitution[_]] = List()

    def ~:(frag: Fragment): Fragment = frag match {
        case TextFrag(more) => TextFrag(more + text)
        case _ => SequenceFrag(frag :: List(this))
    }
}

final case class SubstitutionFrag(val substitution: Substitution[_]) extends Fragment {
    def ~:(frag: Fragment): Fragment = SequenceFrag(frag :: List(this))

    def text: String = "?"

    def substitutions: Seq[Substitution[_]] = List(substitution)
}

final case class SequenceFrag(val fragments: List[Fragment]) extends Fragment {
    def text: String = fragments.map(_.text).mkString("", "", "")

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

trait Resolver {

    def apply[T](slot: Slot[T]): Option[T]
}

sealed trait Substitution[T] {

    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver): Unit
}

final object Bindings {

    final case class Binding[T](slot: Slot[T], value: Option[T])

    val Empty = new Bindings(Map())

    def apply(pairs: Binding[_]*): Bindings =
        Empty ++ pairs
}

final class Bindings private(private val map: Map[Slot[_], Option[_]]) extends Resolver {

    def +[T](pair: Bindings.Binding[T]): Bindings =
        new Bindings(map.updated(pair.slot, pair.value))

    def ++(pairs: Seq[Bindings.Binding[_]]): Bindings =
        new Bindings(pairs.foldLeft(map)((m, p) => m.updated(p.slot, p.value)))

    def -[T](key: Slot[T]): Bindings =
        new Bindings(map - key)

    def get[T](key: Slot[T]): Option[Option[T]] =
        map.get(key).map(_.asInstanceOf[Option[T]])

    def apply[T](key: Slot[T]): Option[T] =
        map(key).asInstanceOf[Option[T]]

    def contains(key: Slot[_]): Boolean =
        map.contains(key)

    def filter(keys: Set[Slot[_]]): Bindings =
        new Bindings(map.foldLeft(Map[Slot[_], Option[_]]())((nmap, pair) => {
            if (keys contains pair._1) nmap + pair else nmap
        }))

    def merge(other: Bindings): Bindings =
        new Bindings(other.map ++ map)
}

final class Template private[jdbc](val text: String, val substitutions: Seq[Substitution[_]]) {

    lazy val slots: Set[Slot[_]] =
        Set(substitutions.collect({ case s: Slot[_] => s }): _*)

    override def toString: String =
        "Command(" + text + "," + substitutions + ")"

    def executeQuery[U](connection: Connection, bindings: Resolver)(fn: (ResultSet) => U): U = {

        val stmt = connection.prepareStatement(text, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)

        try {

            var index = 1

            substitutions.foreach((s) => {
                s.bindParameter(stmt, index, bindings)
                index += 1
            })

            val rs = stmt.executeQuery()

            try {

                fn(rs)

            } finally
                rs.close

        } finally
            stmt.close
    }

    def executeInsert[U](connection: Connection, bindings: Resolver)(fn: (ResultSet) => U): U = {

        val stmt = connection.prepareStatement(text, Statement.RETURN_GENERATED_KEYS)

        try {

            var index = 1

            substitutions.foreach((s) => {
                s.bindParameter(stmt, index, bindings)
                index += 1
            })

            stmt.executeUpdate()
            val rs = stmt.getGeneratedKeys()

            try {

                fn(rs)

            } finally
                rs.close

        } finally
            stmt.close
    }

    def executeCommand(connection: Connection, bindings: Resolver): Int = {

        val stmt = connection.prepareStatement(text, Statement.NO_GENERATED_KEYS)

        try {

            var index = 1

            substitutions.foreach((s) => {
                s.bindParameter(stmt, index, bindings)
                index += 1
            })

            stmt.executeUpdate()

        } finally
            stmt.close
    }
}