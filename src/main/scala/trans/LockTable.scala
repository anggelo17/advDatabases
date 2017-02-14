package trans
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
<<<<<<< HEAD
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListMap
=======
import Cycle._
>>>>>>> origin/master
/**
  * Created by supriya on 2/9/17.
  */
object LockTable {
<<<<<<< HEAD
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
=======
class Lock(tid: Int) {
  val lock = new ReentrantReadWriteLock()
  val transid=tid
}
  var lockmap = new mutable.HashMap[Int,Lock]()

  def rl(oid: Int,tid: Int) =
  {
    if(!(lockmap.keySet.contains(oid))) {
      lockmap += (oid -> new Lock(tid))
      lockmap(oid).lock.readLock().lock()
    }
    else {
     if(checkCycle(3,tid,lockmap(oid).transid))

    }
    //println("======="+lockmap(oid).lock.toString)
  }
  def wl(oid: Int,tid: Int) = {
    if (!lockmap.keySet.contains(oid))
      lockmap += (oid) -> new Lock(tid)
    lockmap(oid).lock.writeLock().lock()
    //println("======="+lockmap(oid).lock.toString)
  }
    def ul(oid: Int): Unit = {
      lockmap(oid).lock.writeLock().unlock()
    }
   def checkCycle(noTrans: Int,tid1 :Int,tid2: Int): Boolean ={
   val g=new Graph(Array.fill(noTrans) (Set[Int]()))
     g.ch(tid1) += tid2
     hasCycle(g)
   }
  }
>>>>>>> origin/master
