package darts.lib.sql.jdbc

object Utilities {
    
    import Fragment._
	
    private def generateOneOf[T](eqOp: String, inOp: String, seq: Iterable[T])(implicit tp: Type[T]): Fragment = {
        val iter = seq.iterator
        val first = iter.next
        if (!iter.hasNext) eqOp ~: constant(Some(first))
        else {
            val head = inOp ~: "(" ~: constant(Some(first))
            val middle = iter.foldRight(head) { (elt, hd) => hd ~: ", " ~: constant(Some(elt)) }
            middle ~: ")"
        }
    }
    
    def oneOf[T](seq: Iterable[T])(implicit tp: Type[T]): Fragment =
        generateOneOf(" = ", " IN ", seq)
        
    def notOneOf[T](seq: Iterable[T])(implicit tp: Type[T]): Fragment =
        generateOneOf(" <> ", " NOT IN ", seq)
      
    type Closeable = { def close(): Unit } 
        
    def closing[E <: Closeable, U](resource: E)(body: =>U): U = {
        try body finally resource.close
    }
}


