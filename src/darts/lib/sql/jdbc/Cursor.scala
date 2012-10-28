package darts.lib.sql.jdbc

import java.sql.ResultSet

sealed trait Column[T] {

    def descriptor: Type[T]
    def apply(rs: ResultSet): Option[T]
    def required = new NotNullReader(this, (_: Option[T]).get)
    def forced(fn: Option[T]=>T) = new NotNullReader(this, fn) 
}

final case class IndexedColumn[T] (val index: Int, val descriptor: Type[T]) extends Column[T] {
    def apply(rs: ResultSet): Option[T] = descriptor.readValue(rs, index)
}

final case class NamedColumn[T] (val name: String, val descriptor: Type[T]) extends Column[T] {
    def apply(rs: ResultSet): Option[T] = descriptor.readValue(rs, name)
}

final case class NotNullReader[T] (val reader: Column[T], val forcer: (Option[T])=>T) {
    def descriptor: Type[T] = reader.descriptor
    def apply(rs: ResultSet): T = forcer(reader(rs))
}

object Column {
    
    def apply[T](name: String, desc: Type[T]): Column[T] = NamedColumn(name, desc)
    def apply[T](index: Int, desc: Type[T]): Column[T] = IndexedColumn(index, desc)
}

trait Cursor[+T] {
    
    def next: Boolean
    def get: T
}

abstract class BasicCursor[T] (private val underlying: ResultSet) extends Cursor[T] {

    private var current: T = _
    private var available: Boolean = false
    
    protected def read(rs: ResultSet): T
    
    final def next: Boolean = {
        if (!underlying.next) {
            available = false
            false
        } else {
        	current = read(underlying)
        	available = true
        	true
        }
    }
    
    final def get: T =
        if (!available) throw new IllegalStateException
        else current
}