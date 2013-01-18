package darts.lib

import javax.sql.DataSource
package object sql {
    
    type Connection = java.sql.Connection
    type ResultSet = java.sql.ResultSet
    type PreparedStatement = java.sql.PreparedStatement
    type DataSource = javax.sql.DataSource

    type Type[T] = jdbc.Type[T]
    type TypeDecorator[S,T] = jdbc.TypeDecorator[S,T]
    
    type Slot[T] = jdbc.Slot[T]
    type Bindings = jdbc.Bindings
    type Fragment = jdbc.Fragment
    type Template = jdbc.Template
    type Cursor[T] = jdbc.Cursor[T]
    type BasicCursor[T] = jdbc.BasicCursor[T]
    type Query[T] = jdbc.Query[T]
    type Insert[T] = jdbc.Insert[T]
    type SimpleQuery[T] = jdbc.SimpleQuery[T]
    type BasicQuery[T] = jdbc.BasicQuery[T]
    type SimpleInsert[T] = jdbc.SimpleInsert[T]
    type SimpleAction = jdbc.SimpleAction
    type Column[T] = jdbc.Column[T]
    type DeferredResult[T] = jdbc.DeferredResult[T]
    type Action = jdbc.Action
    type Session = jdbc.Session
    type SessionFactory = jdbc.SessionFactory
    type DataSourceSessionFactory = jdbc.DataSourceSessionFactory
    type Registry[F] = jdbc.Registry[F]
    type ExplicitRegistry[F] = jdbc.ExplicitRegistry[F]
    type Protocol[T <: AnyRef] = jdbc.Protocol[T]
    type Receipt[F,T <: AnyRef] = jdbc.Receipt[F,T]
    type BasicReceipt[F,T <: AnyRef] = jdbc.BasicReceipt[F,T]
    
    val Type = jdbc.Type
    val Slot = jdbc.Slot
    val Bindings = jdbc.Bindings
    val Fragment = jdbc.Fragment
    val Column = jdbc.Column
    val SimpleQuery = jdbc.SimpleQuery
    val SimpleInsert = jdbc.SimpleInsert
    val SimpleAction = jdbc.SimpleAction
    val Protocol = jdbc.Protocol
    val Session = jdbc.Session
    
    val QueryUtilities = jdbc.Utilities
    
    def column[T](name: String)(implicit desc: Type[T]): Column[T] = Column(name, desc) 
    def column[T](index: Int)(implicit desc: Type[T]): Column[T] = Column(index, desc)
}