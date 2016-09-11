package darts.lib.sql.jdbc

trait DeferredResult[+T] extends Traversable[T] {

    def scroll[U](fn: (Cursor[T]) => U): U

    def unique: Option[T] = scroll { cur =>
        if (!cur.next) None
        else {
            val answer = cur.get
            if (cur.next) throw new IllegalStateException
            else Some(answer)
        }
    }

    def first: Option[T] = scroll { cur =>
        if (!cur.next) None else Some(cur.get)
    }

    override def foreach[U](fn: (T) => U): Unit = scroll { cur =>
        while (cur.next) fn(cur.get)
    }
}