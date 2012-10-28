package darts.lib.sql.jdbc

import java.sql.{Array => _, _}
import javax.sql.DataSource

sealed trait SessionOpenMode {

    def configure(connection: Connection): Connection
    
    def + (char: SessionCharacteristics): SessionCharacteristics =
        SessionCharacteristics(this, char.autoCommit)
        
    def + (char: SessionAutoCommit): SessionCharacteristics =
        SessionCharacteristics(this, char)
}

sealed trait SessionAutoCommit {
    
    def configure(connection: Connection): Connection
    
    def + (char: SessionCharacteristics): SessionCharacteristics =
        SessionCharacteristics(char.openMode, this)
        
    def + (char: SessionOpenMode): SessionCharacteristics =
        SessionCharacteristics(char, this)
}

object SessionOpenMode {
    
    final case object ReadOnly extends SessionOpenMode {
        def configure(connection: Connection): Connection = {
            connection.setReadOnly(true)
            connection
        }
    }

    final case object ReadWrite extends SessionOpenMode {
        def configure(connection: Connection): Connection = {
            connection.setReadOnly(false)
            connection
        }
    }
}

object SessionAutoCommit {
    
    final case object Enabled extends SessionAutoCommit {
        def configure(connection: Connection): Connection = {
            connection.setAutoCommit(true)
            connection
        }
    }
    
    final case object Disabled extends SessionAutoCommit {
        def configure(connection: Connection): Connection = {
            connection.setAutoCommit(false)
            connection
        }
    }
}

final case class SessionCharacteristics (val openMode: SessionOpenMode, val autoCommit: SessionAutoCommit) {
    
	def configure(connection: Connection): Connection =
		openMode.configure(autoCommit.configure(connection))
		
	def + (mode: SessionOpenMode): SessionCharacteristics =
	    SessionCharacteristics(mode, autoCommit)
	    
    def + (auto: SessionAutoCommit): SessionCharacteristics =
	    SessionCharacteristics(openMode, auto)
}

object SessionCharacteristics {
    
    def Default = SessionCharacteristics(SessionOpenMode.ReadWrite, SessionAutoCommit.Disabled)
}

trait Session {
    
    val underlying: Connection
    
    def close = underlying.close
    def commit = underlying.commit
    def rollback = underlying.rollback
    
    def apply[T <: AnyRef](protocol: Protocol[T]): T = get(protocol).get
    def apply[T <: AnyRef](protocol: Receipt[Session,T]): T = getOrElse(protocol.protocol, Some(protocol.make(this))).get
    def get[T <: AnyRef](protocol: Protocol[T]): Option[T] = getOrElse(protocol, None)
    def getOrElse[T <: AnyRef](protocol: Protocol[T], fallback: =>Option[T]): Option[T]
    
    def transactionally[U](fn: =>U): U = {
        try {
            val r = fn
            underlying.commit
            r
        } catch {
            case err => { underlying.rollback; throw err }
        }
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
    
	def withSession[U](char: SessionCharacteristics)(fn: (Session)=>U): U
}

abstract class BasicSessionFactory 
extends SessionFactory {
    
    protected def registry: Registry[Session]
    protected def openConnection: Connection 
    
    def withSession[U](char: SessionCharacteristics)(fn: (Session)=>U): U = {
        val cnx = openConnection
        try fn(makeSession(char.configure(cnx))) finally cnx.close
    }
    
    protected def makeSession(cnx: Connection): Session =
        new SessionImpl(cnx)

	protected class SessionImpl (val underlying: Connection)
	extends Session {
    	
        private val cache = registry.newCache(this)

        def getOrElse[T <: AnyRef](protocol: Protocol[T], fallback: =>Option[T]): Option[T] = 
            cache.getOrElse(protocol, fallback)
    }
}

abstract class DataSourceSessionFactory 
extends BasicSessionFactory {
    
	protected def dataSource: DataSource
	protected def openConnection: Connection = dataSource.getConnection()
}