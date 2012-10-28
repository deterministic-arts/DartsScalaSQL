package darts.lib.sql.jdbc

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.HashMap
import scala.annotation.tailrec

object Registry {
    
    type Factory[F,T] = Function1[F,T]
}

final class Protocol[T <: AnyRef] private (val qualifier: String, val manifest: Manifest[T]) {

    override def equals(ob: Any): Boolean = ob match {
        case p: Protocol[_] => manifest == p.manifest && qualifier == p.qualifier
        case _ => false
    }
    
    override def hashCode: Int =
        manifest.hashCode * 31 + qualifier.hashCode
        
    override def toString: String = 
        "Protocol[" + manifest + "](" + qualifier + ")"
}

object Protocol {
    
    def default[T <: AnyRef](implicit m: Manifest[T]) = apply("")
    def apply[T <: AnyRef](qual: String)(implicit m: Manifest[T]): Protocol[T] = new Protocol(qual, m)
    def unapply(ob: Any): Option[(String,Manifest[_])] = ob match {
        case p: Protocol[_] => Some((p.qualifier, p.manifest))
        case _ => None
    }
}

trait Receipt[F,T <: AnyRef] {
    
    val protocol: Protocol[T]
    def make(ob: F): T
}

abstract class BasicReceipt[F,T <: AnyRef] (val protocol: Protocol[T]) extends Receipt[F,T] {
    def this()(implicit mf: Manifest[T]) = this(Protocol.default[T])
}

abstract class Registry[F] { outer =>
    
    import Registry._

    def apply[T <: AnyRef](k: Protocol[T]): Factory[F,T] =
        get(k).get
    
    def get[T <: AnyRef](k: Protocol[T]): Option[Factory[F,T]]
    
    def newCache(target: F): Cache = 
    	new Cache(target)
    
    final class Cache private[Registry] (private val target: F) {
        
        private val lock = new ReentrantLock
        private val cache = new HashMap[Protocol[_],AnyRef]
        
        def apply[T <: AnyRef](k: Protocol[T]): T =
            get(k).get
            
        def apply[T <: AnyRef](k: Receipt[F,T]): T =
            getOrElse(k.protocol, Some(k.make(target))).get
            
        def getOrElse[T <: AnyRef](k: Protocol[T], fb: =>Option[T]): Option[T] = {
            lock.lock
            try {
                cache.get(k) match {
                    case Some(ob) => Some(ob.asInstanceOf[T])
                    case None => {
                        outer.get(k) match {
                            case None => {
                                val ad = fb
                                if (!ad.isEmpty) cache(k) = ad.get
                                ad
                            }
                            case Some(fc) => {
                                val ad = fc(target)
                                cache(k) = ad
                                Some(ad)
                            }
                        }
                    }
                }
            } finally
            	lock.unlock
        }
        
        def get[T <: AnyRef](k: Protocol[T]): Option[T] = getOrElse(k, None)
    }
}

final class ExplicitRegistry[F] extends Registry[F] {
    
    import Registry._
    
    private val cell = new AtomicReference(Map[Protocol[_],Factory[F,_]]())
    
    override def get[T <: AnyRef](k: Protocol[T]): Option[Factory[F,T]] =
    	cell.get.get(k).map(_.asInstanceOf[Factory[F,T]])
    
    def update[T <: AnyRef](k: Protocol[T], fn: Factory[F,T]) {
        @tailrec def loop(m: Map[Protocol[_],Factory[F,_]]) {
            m.get(k) match {
                case Some(af) =>
                    if (af != fn) throw new IllegalStateException
                    else ()
                case None => 
                    if (!cell.compareAndSet(m, m + (k -> fn))) loop(cell.get)
                    else ()
            }
        }
        loop(cell.get)
    }
}