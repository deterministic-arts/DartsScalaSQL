package darts.lib.sql.jdbc

import java.sql.{PreparedStatement, Connection, ResultSet, Statement}

sealed trait Fragment {
	
    def ~ (frag: Fragment): Fragment
    def ~: (frag: Fragment): Fragment = frag ~ this
    
    def text: String
    def substitutions: Seq[Substitution[_]]
    
    def toTemplate: Template =
        new Template(text, substitutions)
}

trait FragmentBuilder {
    
    def toFragment: Fragment
}

object Fragment {

    implicit def fragmentFromString(text: String): Fragment = LiteralFrag(text)
    implicit def fragmentFromSubstitution[T](subst: Substitution[T]): Fragment = SubstitutionFrag(subst)
    implicit def fragmentFromBuilder(builder: FragmentBuilder): Fragment = builder.toFragment

    def constant[T](value: Option[T])(implicit tp: Type[T]): Fragment = SubstitutionFrag(Constant(value, tp))
}

final class Template private[jdbc] (val text: String, val substitutions: Seq[Substitution[_]]) {
    
    lazy val slots: Set[Slot[_]] = Set(substitutions.collect({ case s: Slot[_] => s}): _*)
    
    override def toString: String = 
        "Template(" + text + "," + substitutions + ")"
    
    def executeQuery[U](connection: Connection, bindings: Resolver)(fn: (ResultSet)=>U): U = {

		val stmt = connection.prepareStatement(text, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)

		try {

			var index = 1
        
			substitutions.foreach((s)=> {
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
    
    def executeInsert[U](connection: Connection, bindings: Resolver)(fn: (ResultSet)=>U): U = {
        
        val stmt = connection.prepareStatement(text, Statement.RETURN_GENERATED_KEYS)

		try {

			var index = 1
        
			substitutions.foreach((s)=> {
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
        
			substitutions.foreach((s)=> {
				s.bindParameter(stmt, index, bindings)
				index += 1
			})

			stmt.executeUpdate()
    
		} finally 
			stmt.close
    }    
}

final case class LiteralFrag (val text: String) extends Fragment {

    def substitutions: Seq[Substitution[_]] = List()

    def ~ (frag: Fragment): Fragment = frag match {
        case SequenceFrag(ls) => SequenceFrag(this :: ls)
        case _ => SequenceFrag(List(this, frag))
    }
}

final case class SubstitutionFrag[T] (val substitution: Substitution[T]) extends Fragment {
    
    def substitutions: Seq[Substitution[_]] = List(substitution)
    def text = "?"
        
    def ~ (frag: Fragment): Fragment = frag match {
        case SequenceFrag(ls) => SequenceFrag(this :: ls)
        case _ => SequenceFrag(List(this, frag))
    }
}

final case class SequenceFrag (val children: List[Fragment]) extends Fragment {
    
    def substitutions: Seq[Substitution[_]] = children.flatMap(_.substitutions)
    def text = children.flatMap(_.text).mkString("", "", "")
    
    def ~ (frag: Fragment): Fragment = frag match {
        case SequenceFrag(ls) => SequenceFrag(children ::: ls)
        case _ => SequenceFrag(children ::: List(frag))
    }
}

sealed trait Substitution[T] {
    
    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver): Unit
}

trait Resolver {
    
    def apply[T](slot: Slot[T]): Option[T]
}

final case class Constant[T] (val value: Option[T], val descriptor: Type[T]) extends Substitution[T] {
    
    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver) {
        descriptor.bindValue(stmt, index, value)
    }
}

final case class Slot[T] (val name: String, val descriptor: Type[T]) extends Substitution[T] {
    
    def bindParameter(stmt: PreparedStatement, index: Int, bindings: Resolver) {
        descriptor.bindValue(stmt, index, bindings(this))
    }
    
    def bind(value: Option[T]): Bindings.Binding[T] = (this, value)
    def apply(value: T): Bindings.Binding[T] = bind(Some(value))
    lazy val empty: Bindings.Binding[T] = bind(None)
}

final object Bindings {
    
    type Binding[T] = Tuple2[Slot[T],Option[T]]
    
    val Empty = new Bindings(Map())

    def apply(pairs: Binding[_]*): Bindings =
        if (pairs.isEmpty) Empty else new Bindings(Map(pairs: _*))
}

final class Bindings private (private val map: Map[Slot[_],Option[_]]) extends Resolver {
    
	def +[T] (pair: Bindings.Binding[T]): Bindings =
		new Bindings(map + pair)
	
	def ++ (pairs: Seq[Bindings.Binding[_]]): Bindings =
	    new Bindings(map ++ pairs)
    
	def -[T] (key: Slot[T]): Bindings =
	    new Bindings(map - key)
	
	def get[T] (key: Slot[T]): Option[Option[T]] = 
	    map.get(key).map(_.asInstanceOf[Option[T]])
	    
	def apply[T] (key: Slot[T]): Option[T] = 
	    map(key).asInstanceOf[Option[T]]
	
	def contains(key: Slot[_]): Boolean =
	    map.contains(key)
	    
	def filter(keys: Set[Slot[_]]): Bindings =
		new Bindings(map.foldLeft(Map[Slot[_],Option[_]]())((nmap,pair)=> {
			if (keys contains pair._1) nmap + pair else nmap
		}))
	
	def merge(other: Bindings): Bindings =
	    new Bindings(other.map ++ map)
}
