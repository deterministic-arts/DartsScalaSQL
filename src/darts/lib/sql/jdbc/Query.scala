package darts.lib.sql.jdbc

import java.sql.{Connection, ResultSet, PreparedStatement}

abstract class Query[T] {
	
    protected def template: Template
    
    def apply(connection: Connection): DeferredResult[T] =
        new Results(connection, Bindings.Empty)
    
    def apply(connection: Connection, bindings: Bindings): DeferredResult[T] =
        new Results(connection, bindings)
    
    def apply(connection: Connection, bindings: Bindings.Binding[_]*): DeferredResult[T] =
        new Results(connection, Bindings(bindings: _*))
 
    protected def makeRowReader(rs: ResultSet): Cursor[T]
    
    private final class Results (val connection: Connection, val parameters: Bindings) 
    extends DeferredResult[T] {
    	override def scroll[U](fn: (Cursor[T])=>U): U = {
    	    template.executeQuery(connection, parameters) { rs => fn(makeRowReader(rs)) }
    	}
    }
}

abstract class BasicQuery[T] extends Query[T] {
    
    protected def readRow(rs: ResultSet): T
    
    protected def makeRowReader(rs: ResultSet): Cursor[T] =
        new RowReader(rs)
    
    private class RowReader private[BasicQuery] (rs: ResultSet)
    extends BasicCursor[T](rs) {
        protected def read(rs: ResultSet): T = readRow(rs)
    }
}

abstract class Insert[T] {
	
    protected def template: Template
    protected def readRow(rs: ResultSet): T
    
    def apply(connection: Connection): Seq[T] =
        apply(connection, Bindings.Empty)

    def apply(connection: Connection, bindings: Bindings.Binding[_]*): Seq[T] =
        apply(connection, Bindings(bindings: _*))
    
    def apply(connection: Connection, bindings: Bindings): Seq[T] =
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

final class SimpleQuery[T] (protected override val template: Template, private val reader: (ResultSet)=>T) extends BasicQuery[T] {
    
    def this(frag: Fragment, reader: (ResultSet)=>T) = this(frag.toTemplate, reader)
    protected def readRow(rs: ResultSet): T = reader(rs)
}

object SimpleQuery {
    
    def apply[T](template: Template)(reader: (ResultSet)=>T): SimpleQuery[T] = new SimpleQuery(template, reader)
    def apply[T](template: Fragment)(reader: (ResultSet)=>T): SimpleQuery[T] = new SimpleQuery(template.toTemplate, reader)
}

final class SimpleInsert[T] (protected override val template: Template, private val reader: (ResultSet)=>T) extends Insert[T] {
    
    def this(frag: Fragment, reader: (ResultSet)=>T) = this(frag.toTemplate, reader)
    protected def readRow(rs: ResultSet): T = reader(rs)
}

object SimpleInsert {
    
    def apply[T](template: Template)(reader: (ResultSet)=>T): SimpleInsert[T] = new SimpleInsert(template, reader)
    def apply[T](template: Fragment)(reader: (ResultSet)=>T): SimpleInsert[T] = new SimpleInsert(template.toTemplate, reader)
}