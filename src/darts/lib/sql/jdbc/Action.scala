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

    protected def template: Template

    protected def execute(connection: Connection, bindings: Bindings): Int =
        template.executeCommand(connection, bindings)

    override def toString: String =
        "Action(" + template + ")"
}

final class SimpleAction(protected override val template: Template)
    extends Action with ApplyableAction {

    def this(frag: Fragment) = this(frag.toTemplate)
}

object SimpleAction {

    def apply(frag: Fragment): SimpleAction = new SimpleAction(frag.toTemplate)

    def apply(frag: Template): SimpleAction = new SimpleAction(frag)
}