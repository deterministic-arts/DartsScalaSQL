package darts.lib.sql.jdbc

import java.sql.{Array => _, _}
import javax.sql.DataSource

sealed trait SessionOpenMode {

    def configure(connection: Connection): Connection

    def +(char: SessionCharacteristics): SessionCharacteristics =
        SessionCharacteristics(this, char.autoCommit)

    def +(char: SessionAutoCommit): SessionCharacteristics =
        SessionCharacteristics(this, char)
}

sealed trait SessionAutoCommit {

    def configure(connection: Connection): Connection

    def +(char: SessionCharacteristics): SessionCharacteristics =
        SessionCharacteristics(char.openMode, this)

    def +(char: SessionOpenMode): SessionCharacteristics =
        SessionCharacteristics(char, this)
}

object SessionOpenMode {

    final case object ReadOnly extends SessionOpenMode {
        def configure(connection: Connection): Connection = {
            if (!connection.isReadOnly) connection.setReadOnly(true)
            connection
        }
    }

    final case object ReadWrite extends SessionOpenMode {
        def configure(connection: Connection): Connection = {
            if (connection.isReadOnly) connection.setReadOnly(false)
            connection
        }
    }

}

object SessionAutoCommit {

    final case object Enabled extends SessionAutoCommit {
        def configure(connection: Connection): Connection = {
            if (!connection.getAutoCommit) connection.setAutoCommit(true)
            connection
        }
    }

    final case object Disabled extends SessionAutoCommit {
        def configure(connection: Connection): Connection = {
            if (connection.getAutoCommit) connection.setAutoCommit(false)
            connection
        }
    }

}

final case class SessionCharacteristics(val openMode: SessionOpenMode, val autoCommit: SessionAutoCommit) {

    def configure(connection: Connection): Connection =
        autoCommit.configure(openMode.configure(connection))

    def +(mode: SessionOpenMode): SessionCharacteristics =
        SessionCharacteristics(mode, autoCommit)

    def +(auto: SessionAutoCommit): SessionCharacteristics =
        SessionCharacteristics(openMode, auto)
}

object SessionCharacteristics {

    def Default = SessionCharacteristics(SessionOpenMode.ReadWrite, SessionAutoCommit.Disabled)
}

trait Session extends Connection with Adaptable[Session] {

    def transactionally[U](fn: => U): U = {
        var ok: Boolean = false
        try {
            val r = fn
            ok = true
            r
        } finally
            if (ok) commit()
            else rollback()
    }
}

object Session {

    type Characteristics = SessionCharacteristics
    type OpenMode = SessionOpenMode
    type AutoCommit = SessionAutoCommit

    val Characteristics = SessionCharacteristics
    val OpenMode = SessionOpenMode
    val AutoCommit = SessionAutoCommit
}

trait SessionFactory {

    def withSession[U](char: SessionCharacteristics)(fn: (Session) => U): U

    def openSession: Session
}

abstract class BasicSessionFactory
    extends SessionFactory {

    protected def openConnection: Connection

    def withSession[U](char: SessionCharacteristics)(fn: (Session) => U): U = {
        val cnx = openConnection
        try {
            fn(makeSession(char.configure(cnx)))
        } finally cnx.close
    }

    def openSession: Session =
        makeSession(openConnection)

    protected def makeSession(cnx: Connection): Session =
        new SessionImpl(cnx)

    protected class SessionImpl(under: Connection)
        extends delegate.DelegateConnection(under) with Session {

        val adapters = new AdapterCache[Session](this)

        override def isWrapperFor(c: Class[_]): Boolean =
            c.isInstance(this) || connection.isWrapperFor(c)

        override def unwrap[T](c: Class[T]): T =
            if (c.isInstance(this)) c.cast(this)
            else connection.unwrap(c)
    }

}

abstract class DataSourceSessionFactory
    extends BasicSessionFactory {

    protected def dataSource: DataSource

    protected def openConnection: Connection = dataSource.getConnection()
}