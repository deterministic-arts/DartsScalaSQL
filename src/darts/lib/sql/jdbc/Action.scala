package darts.lib.sql.jdbc

import java.sql.Connection

trait ApplyableAction {
    self: Action =>

    def apply(bindings: Bindings)(implicit connection: Connection): Int =
        execute(connection, bindings)

    def apply(bindings: Bindings.Binding[_]*)(implicit connection: Connection): Int =
        execute(connection, Bindings(bindings: _*))
}

abstract class Action {

    def template: Template

    protected def execute(connection: Connection, bindings: Bindings): Int =
        template.executeCommand(connection, bindings)

    override def toString: String =
        "Action(" + template + ")"
}

final class SimpleAction(override val template: Template)
    extends Action with ApplyableAction {

    def this(frag: Fragment) = this(new Template(frag))
}

object SimpleAction {

    def apply(frag: Fragment): SimpleAction = new SimpleAction(new Template(frag))

    def apply(frag: Template): SimpleAction = new SimpleAction(frag)
}