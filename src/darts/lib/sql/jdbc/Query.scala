package darts.lib.sql.jdbc

import java.sql.{Connection, ResultSet}


trait ApplyableQuery[T] {
    self: Query[T] =>

    def apply(bindings: Bindings)(implicit connection: Connection): DeferredResult[T] =
        execute(connection, bindings)

    def apply(bindings: Bindings.Binding[_]*)(implicit connection: Connection): DeferredResult[T] =
        execute(connection, Bindings(bindings: _*))
}

abstract class Query[T] {

    protected def template: Template

    protected def execute(connection: Connection, bindings: Bindings): DeferredResult[T] =
        new Results(connection, bindings)

    protected def makeRowReader(rs: ResultSet): Cursor[T]

    private final class Results(val connection: Connection, val parameters: Bindings)
        extends DeferredResult[T] {
        override def scroll[U](fn: (Cursor[T]) => U): U = {
            template.executeQuery(connection, parameters) { rs => fn(makeRowReader(rs)) }
        }
    }

}

abstract class BasicQuery[T] extends Query[T] {

    protected def readRow(rs: ResultSet): T

    protected def makeRowReader(rs: ResultSet): Cursor[T] =
        new RowReader(rs)

    private class RowReader private[BasicQuery](rs: ResultSet)
        extends BasicCursor[T](rs) {
        protected def read(rs: ResultSet): T = readRow(rs)
    }

}

trait ApplyableInsert[T] {
    self: Insert[T] =>

    def apply(bindings: Bindings)(implicit connection: Connection): Seq[T] =
        execute(connection, bindings)

    def apply(bindings: Bindings.Binding[_]*)(implicit connection: Connection): Seq[T] =
        execute(connection, Bindings(bindings: _*))
}

abstract class Insert[T] {

    protected def template: Template

    protected def readRow(rs: ResultSet): T

    protected def execute(connection: Connection, bindings: Bindings): Seq[T] =
        template.executeInsert(connection, bindings)(fetchIds)

    override def toString: String =
        "Insert(" + template + ")"

    private def fetchIds(rs: ResultSet): Seq[T] = {
        import scala.collection.immutable.VectorBuilder
        val buf = new VectorBuilder[T]
        while (rs.next) buf += readRow(rs)
        buf.result()
    }
}

final class SimpleQuery[T](protected override val template: Template, private val reader: (ResultSet) => T)
    extends BasicQuery[T] with ApplyableQuery[T] {

    def this(frag: Fragment, reader: (ResultSet) => T) = this(frag.toTemplate, reader)

    protected def readRow(rs: ResultSet): T = reader(rs)
}

object SimpleQuery {

    def apply[T](template: Template)(reader: (ResultSet) => T): SimpleQuery[T] = new SimpleQuery(template, reader)

    def apply[T](template: Fragment)(reader: (ResultSet) => T): SimpleQuery[T] = new SimpleQuery(template.toTemplate, reader)
}

final class SimpleInsert[T](protected override val template: Template, private val reader: (ResultSet) => T)
    extends Insert[T] with ApplyableInsert[T] {

    def this(frag: Fragment, reader: (ResultSet) => T) = this(frag.toTemplate, reader)

    protected def readRow(rs: ResultSet): T = reader(rs)
}

object SimpleInsert {

    def apply[T](template: Template)(reader: (ResultSet) => T): SimpleInsert[T] = new SimpleInsert(template, reader)

    def apply[T](template: Fragment)(reader: (ResultSet) => T): SimpleInsert[T] = new SimpleInsert(template.toTemplate, reader)
}