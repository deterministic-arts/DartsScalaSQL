package darts.lib.sql.jdbc

object Utilities {
      
    type Closeable = { def close(): Unit } 
        
    def closing[E <: Closeable, U](resource: E)(body: =>U): U = {
        try body finally resource.close
    }
}


