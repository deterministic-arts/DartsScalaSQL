package darts.lib.sql.jdbc

import java.sql.{PreparedStatement, ResultSet, Timestamp, Types}

sealed abstract class Type[T] {
    outer =>

    type Rep = T

    def readValue(rs: ResultSet, name: String): Option[T]

    def readValue(rs: ResultSet, index: Int): Option[T]

    def bindValue(stmt: PreparedStatement, index: Int, value: Option[T]): Unit
}

abstract class Primitive[T] protected[jdbc]() extends Type[T] {

    protected def typeCode: Int

    protected def doBindValue(stmt: PreparedStatement, index: Int, value: T): Unit

    def bindValue(stmt: PreparedStatement, index: Int, value: Option[T]): Unit =
        if (value.isEmpty) stmt.setNull(index, typeCode) else doBindValue(stmt, index, value.get)
}

abstract class TypeDecorator[S, T](val storedType: Type[S]) extends Type[T] {

    type Stored = S

    protected def fromStorage(value: Stored): Rep

    protected def toStorage(value: Rep): Stored

    def readValue(rs: ResultSet, name: String): Option[Rep] =
        storedType.readValue(rs, name).map(fromStorage)

    def readValue(rs: ResultSet, index: Int): Option[Rep] =
        storedType.readValue(rs, index).map(fromStorage)

    def bindValue(stmt: PreparedStatement, index: Int, value: Option[Rep]): Unit =
        storedType.bindValue(stmt, index, value.map(toStorage))
}

final object Type {

    implicit final case object Byte extends Primitive[Byte] {

        protected val typeCode = Types.TINYINT

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Byte): Unit =
            stmt.setByte(index, value)

        def readValue(rs: ResultSet, name: String): Option[Byte] = {
            val raw = rs.getByte(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Byte] = {
            val raw = rs.getByte(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Short extends Primitive[Short] {

        protected val typeCode = Types.SMALLINT

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Short): Unit =
            stmt.setShort(index, value)

        def readValue(rs: ResultSet, name: String): Option[Short] = {
            val raw = rs.getShort(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Short] = {
            val raw = rs.getShort(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Int extends Primitive[Int] {

        protected val typeCode = Types.INTEGER

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Int): Unit =
            stmt.setInt(index, value)

        def readValue(rs: ResultSet, name: String): Option[Int] = {
            val raw = rs.getInt(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Int] = {
            val raw = rs.getInt(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Long extends Primitive[Long] {

        protected val typeCode = Types.BIGINT

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Long): Unit =
            stmt.setLong(index, value)

        def readValue(rs: ResultSet, name: String): Option[Long] = {
            val raw = rs.getLong(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Long] = {
            val raw = rs.getLong(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Float extends Primitive[Float] {

        protected val typeCode = Types.FLOAT

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Float): Unit =
            stmt.setFloat(index, value)

        def readValue(rs: ResultSet, name: String): Option[Float] = {
            val raw = rs.getFloat(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Float] = {
            val raw = rs.getFloat(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Double extends Primitive[Double] {

        protected val typeCode = Types.DOUBLE

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Double): Unit =
            stmt.setDouble(index, value)

        def readValue(rs: ResultSet, name: String): Option[Double] = {
            val raw = rs.getDouble(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Double] = {
            val raw = rs.getDouble(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Boolean extends Primitive[Boolean] {

        protected val typeCode = Types.BOOLEAN

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Boolean): Unit =
            stmt.setBoolean(index, value)

        def readValue(rs: ResultSet, name: String): Option[Boolean] = {
            val raw = rs.getBoolean(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Boolean] = {
            val raw = rs.getBoolean(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object BigDecimal extends Primitive[BigDecimal] {

        protected val typeCode = Types.DECIMAL

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: BigDecimal): Unit =
            stmt.setBigDecimal(index, value.underlying())

        def readValue(rs: ResultSet, name: String): Option[BigDecimal] = {
            val raw = rs.getBigDecimal(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[BigDecimal] = {
            val raw = rs.getBigDecimal(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object String extends Primitive[String] {

        protected val typeCode = Types.VARCHAR

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: String): Unit =
            stmt.setString(index, value)

        def readValue(rs: ResultSet, name: String): Option[String] = {
            val raw = rs.getString(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[String] = {
            val raw = rs.getString(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Timestamp extends Primitive[Timestamp] {

        protected val typeCode = Types.TIMESTAMP

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: Timestamp): Unit =
            stmt.setTimestamp(index, value)

        def readValue(rs: ResultSet, name: String): Option[Timestamp] = {
            val raw = rs.getTimestamp(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[Timestamp] = {
            val raw = rs.getTimestamp(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Date extends Primitive[java.sql.Date] {

        protected val typeCode = Types.DATE

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: java.sql.Date): Unit =
            stmt.setDate(index, value)

        def readValue(rs: ResultSet, name: String): Option[java.sql.Date] = {
            val raw = rs.getDate(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[java.sql.Date] = {
            val raw = rs.getDate(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Time extends Primitive[java.sql.Time] {

        protected val typeCode = Types.TIME

        protected def doBindValue(stmt: PreparedStatement, index: Int, value: java.sql.Time): Unit =
            stmt.setTime(index, value)

        def readValue(rs: ResultSet, name: String): Option[java.sql.Time] = {
            val raw = rs.getTime(name)
            if (rs.wasNull) None else Some(raw)
        }

        def readValue(rs: ResultSet, name: Int): Option[java.sql.Time] = {
            val raw = rs.getTime(name)
            if (rs.wasNull) None else Some(raw)
        }
    }

    implicit final case object Char extends TypeDecorator[String, Char](String) {

        protected def fromStorage(value: String): Char =
            value.charAt(0)

        protected def toStorage(value: Char): String =
            "" + value
    }

    implicit final case object Instant extends TypeDecorator[Timestamp, java.time.Instant](Timestamp) {

        protected def fromStorage(value: Stored): Rep =
            value.toInstant()

        protected def toStorage(value: Rep): Stored =
            java.sql.Timestamp.from(value)
    }

    implicit final case object LocalDate extends TypeDecorator[java.sql.Date, java.time.LocalDate](Date) {

        protected def fromStorage(value: Stored): Rep =
            value.toLocalDate()

        protected def toStorage(value: Rep): Stored =
            java.sql.Date.valueOf(value)
    }

    implicit final case object LocalTime extends TypeDecorator[java.sql.Time, java.time.LocalTime](Time) {

        protected def fromStorage(value: Stored): Rep =
            value.toLocalTime()

        protected def toStorage(value: Rep): Stored =
            java.sql.Time.valueOf(value)
    }

    implicit final case object LocalDateTime extends TypeDecorator[java.sql.Timestamp, java.time.LocalDateTime](Timestamp) {

        protected def fromStorage(value: Stored): Rep =
            value.toLocalDateTime()

        protected def toStorage(value: Rep): Stored =
            java.sql.Timestamp.valueOf(value)
    }

}