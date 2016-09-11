package darts.lib.sql.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

trait Resolver {

    def apply[T](slot: Slot[T]): Option[T]
}

trait Substitution[T] {

    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver): Unit
}

final class Template (val text: String, val substitutions: Seq[Substitution[_]]) {

    def this(frag: Fragment) = this(frag.text, frag.substitutions)

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