package darts.lib.sql.jdbc

import java.sql.{Connection, ResultSet, PreparedStatement}

abstract class Action {
	
    protected def template: Template
    
    def apply(connection: Connection): Int =
    	template.executeCommand(connection, Bindings.Empty)
        
    def apply(connection: Connection, bindings: Bindings): Int =
        template.executeCommand(connection, bindings)
    
    def apply(connection: Connection, bindings: Bindings.Binding[_]*): Int =
        template.executeCommand(connection, Bindings(bindings: _*))
        
    override def toString: String = 
        "Action(" + template + ")"
}

final class SimpleAction (protected override val template: Template) extends Action {
    
    def this(frag: Fragment) = this(frag.toTemplate)
}

object SimpleAction {
    
    def apply(frag: Fragment): SimpleAction = new SimpleAction(frag.toTemplate)
    def apply(frag: Template): SimpleAction = new SimpleAction(frag)
}