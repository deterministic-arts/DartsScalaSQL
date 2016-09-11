package darts.lib.sql.jdbc

import java.util

trait Adaptable[A] {

    def adapters: AdapterCache[A]
}

trait AdapterKey[-F, +T <: AnyRef] {

    def create(adaptee: F): T

    def apply[U <: F](adaptee: Adaptable[U]): T = adaptee.adapters.get(this)
}

final class AdapterCache[A](val instance: A) {

    private[jdbc] val entries = new util.HashMap[AnyRef, AnyRef]()

    def get[T <: AnyRef](key: AdapterKey[A, T]): T = {
        val ob = entries.get(key)
        if (ob ne null) ob.asInstanceOf[T]
        else {
            val w = key.create(instance)
            entries.put(key, w)
            w
        }
    }
}