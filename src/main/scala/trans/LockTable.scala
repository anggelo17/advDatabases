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
  val owner=tid
}
  var lockmap = new mutable.HashMap[Int,Lock]()
  val nTrans = 5
  val waitgraph = new Graph(Array.fill(nTrans)(Set[Int]()))

  def rl(oid: Int,tid: Int,trans: Transaction) =
  {
    var ownerTid = -1
    var wait =false
    if(!(lockmap.keySet.contains(oid)))
      lockmap += (oid -> new Lock(tid))
      //lockmap(oid).lock.readLock().lock()

    if(tid != lockmap(oid).owner)
      wait = true
    ownerTid = lockmap(oid).owner
    if(wait) { waitgraph.ch(tid) += ownerTid}

    if(LockTable.checkCycle())
      {
        trans.rollbackid = true
        if(wait) waitgraph.ch(tid) -= ownerTid
      }
    lockmap(oid).lock.readLock().lock() //tid got the readlock


    //println("======="+lockmap(oid).lock.toString)
  }
  def wl(oid: Int,tid: Int,trans: Transaction) = {
    var wait = false
    var ownerTid = -1
    if (!lockmap.keySet.contains(oid))
      lockmap += (oid) -> new Lock(tid)
    if(lockmap(oid).owner != tid)
      wait = true
    ownerTid = lockmap(oid).owner
    if(wait)
      {
        waitgraph.ch(tid) += ownerTid
      }
    if(LockTable.checkCycle())
      {
        trans.rollbackid=true
        if(wait)
        waitgraph.ch(tid) -= ownerTid
      }
    lockmap(oid).lock.writeLock().lock()
    //println("======="+lockmap(oid).lock.toString)
  }
    def ul(op: Char,oid: Int): Unit = {
      if(lockmap.keySet.contains(oid)) {
        if (op=='w')
          lockmap(oid).lock.writeLock().unlock()
        else
          lockmap(oid).lock.readLock().unlock()
      }
    }
   def checkCycle(): Boolean ={
     hasCycle(waitgraph)
   }
  }
