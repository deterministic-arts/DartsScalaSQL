package darts.lib

import darts.lib.sql.jdbc.{Constant, Slot, Type}

package object sql {

    type Connection = java.sql.Connection
    type ResultSet = java.sql.ResultSet
    type PreparedStatement = java.sql.PreparedStatement
    type DataSource = javax.sql.DataSource

    type Type[T] = jdbc.Type[T]
    type TypeDecorator[S, T] = jdbc.TypeDecorator[S, T]

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
    type AdapterKey[F, T <: AnyRef] = jdbc.AdapterKey[F, T]
    type AdapterCache[F] = jdbc.AdapterCache[F]
    type Adaptable[F] = jdbc.Adaptable[F]

    val Type = jdbc.Type
    val Slot = jdbc.Slot
    val Bindings = jdbc.Bindings
    val Fragment = jdbc.Fragment
    val Column = jdbc.Column
    val SimpleQuery = jdbc.SimpleQuery
    val SimpleInsert = jdbc.SimpleInsert
    val SimpleAction = jdbc.SimpleAction
    val Session = jdbc.Session

    val QueryUtilities = jdbc.Utilities

    def column[T](name: String)(implicit desc: Type[T]): Column[T] =
        Column(name, desc)

    def column[T](index: Int)(implicit desc: Type[T]): Column[T] =
        Column(index, desc)

    def slot[T](name: String)(implicit descriptor: Type[T]): Slot[T] =
        Slot(name, descriptor)

    def constant[T](value: T)(implicit descriptor: Type[T]): Constant[T] =
        Constant(Some(value), descriptor)

    def missing[T](implicit descriptor: Type[T]): Constant[T] =
        Constant(None, descriptor)

    implicit def sqlstr(context: StringContext): jdbc.SqlStringContextExtensions =
        new jdbc.SqlStringContextExtensions(context)
}