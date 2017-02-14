package trans
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListMap
/**
  * Created by supriya on 2/9/17.
  */
object LockTable {
  class Lock {
    val lock = new ReentrantReadWriteLock()
  }
  var lockmap = new mutable.HashMap[Int,Lock]()
  //lockmap += 0-> new Lock()

  def rl(oid: Int) =
  {
    if(!(lockmap.get(oid)==null))
      lockmap += (oid -> new Lock())
    lockmap(oid).lock.readLock().lock()
    println("======="+lockmap(oid).lock.toString)
  }
  def wl(oid: Int) = {
    if (!lockmap.keySet.contains(oid))
      lockmap += (oid) -> new Lock()
    lockmap(oid).lock.writeLock().lock()
    println("======="+lockmap(oid).lock.toString)
  }
  def ul(oid: Int): Unit = {
    lockmap(oid).lock.writeLock().unlock()
  }
}
