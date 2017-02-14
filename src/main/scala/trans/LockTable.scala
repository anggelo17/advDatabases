package trans
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import Cycle._
/**
  * Created by supriya on 2/9/17.
  */
object LockTable {
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
