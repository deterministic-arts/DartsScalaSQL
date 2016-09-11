package darts.lib.sql.jdbc

final class Bindings private(private val map: Map[Slot[_], Option[_]]) extends Resolver {

    def +[T](pair: Bindings.Binding[T]): Bindings =
        new Bindings(map.updated(pair.slot, pair.value))

    def ++(pairs: Seq[Bindings.Binding[_]]): Bindings =
        new Bindings(pairs.foldLeft(map)((m, p) => m.updated(p.slot, p.value)))

    def -[T](key: Slot[T]): Bindings =
        new Bindings(map - key)

    def get[T](key: Slot[T]): Option[Option[T]] =
        map.get(key).map(_.asInstanceOf[Option[T]])

    def apply[T](key: Slot[T]): Option[T] =
        map(key).asInstanceOf[Option[T]]

    def contains(key: Slot[_]): Boolean =
        map.contains(key)

    def filter(keys: Set[Slot[_]]): Bindings =
        new Bindings(map.foldLeft(Map[Slot[_], Option[_]]())((nmap, pair) => {
            if (keys contains pair._1) nmap + pair else nmap
        }))

    def merge(other: Bindings): Bindings =
        new Bindings(other.map ++ map)
}

final object Bindings {

    final case class Binding[T](slot: Slot[T], value: Option[T])

    val Empty = new Bindings(Map())

    def apply(pairs: Binding[_]*): Bindings =
        Empty ++ pairs
}
