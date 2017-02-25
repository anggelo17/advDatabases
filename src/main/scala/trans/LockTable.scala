package trans

import java.lang.Exception
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.script.ScriptException

import scala.collection.mutable
import trans.TrafficLight._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Exception
/**
  * Created by supriya on 2/9/17.
  */
object LockTable {
class Lock(tid: Int) {
  val lock = new ReentrantReadWriteLock()
  var owner=tid
  val transactionIds= new ArrayBuffer[Int]()
}
  var lockmap = new mutable.HashMap[Int,Lock]()
  val nTrans = 5
  val waitgraph = new Graph(Array.fill(nTrans)(Set[Int]()))

  def rl(oid: Int,tid: Int,trans: Transaction):Int =
  {


    if(!(lockmap.keySet.contains(oid))) {
      lockmap += (oid -> new Lock(tid))

      lockmap(oid).lock.readLock().lock()
      lockmap(oid).transactionIds += tid
    }
    else {

      if (lockmap(oid).lock.isWriteLocked){ // if writelocked by any thread

        waitgraph.ch(tid) += lockmap(oid).owner  // edge from tid (read) to write(owner)
        if(LockTable.checkCycle())
        {
          println("there is a cycle????")
          waitgraph.printG()
          trans.rollbackid = true
          waitgraph.ch(tid) -= lockmap(oid).owner
          return 1 //rollback
        }
        //do a wait until write-unlock on oid is unlocked.

      }

      lockmap(oid).lock.readLock().lock()// this will wait
      lockmap(oid).transactionIds += tid


    }



    return 0//ok

    //println("after ..read locks on " + oid + " : " + lockmap(oid).lock.getReadLockCount + " " + lockmap(oid).lock.getReadHoldCount)


    //println("======="+lockmap(oid).lock.toString)
  }
  def wl(oid: Int,tid: Int,trans: Transaction):Int  = {


    println("starting wl on " + oid+" on thread id "+Thread.currentThread().getId)

    if (!lockmap.keySet.contains(oid)) {

      lockmap += (oid) -> new Lock(tid)
    }


    for(i <- 0 until lockmap(oid).transactionIds.size)// draw an edge from tid to  list of transactions ids for that oid ( from write to n reads/ 1 write )
      waitgraph.ch(tid) += lockmap(oid).transactionIds(i)


    if (LockTable.checkCycle()) {
      println("there is a cycle????")
      waitgraph.printG()
      trans.rollbackid = true
      waitgraph.ch(tid) -= lockmap(oid).owner
      return 1// rollback
    }


    if (!lockmap(oid).lock.isWriteLocked)// if not writelocked by any thread
      {
        lockmap(oid).lock.writeLock().lock()
        lockmap(oid).transactionIds +=  tid // should be just one

        waitgraph.ch(tid) -= lockmap(oid).owner // once i got the lock erase the -> dependency from tid to owner.
        lockmap(oid).owner = tid // update the new owner.
      }

    else {//it is write-locked

      // wait and try to lock after
      println("here...wl")
      lockmap(oid).lock.writeLock().lock()
      lockmap(oid).transactionIds +=  tid // should be just one

      waitgraph.ch(tid) -= lockmap(oid).owner // once i got the lock erase the -> dependency from tid to owner.
      lockmap(oid).owner = tid

    }

    return 0// ok


  }
    def ul(op: Char,oid: Int,tid:Int): Unit = {

      println("graph before unlock")
      waitgraph.printG()

      if (oid==1 && op=='w' && tid==1)
        println("aqui")

      if(lockmap.keySet.contains(oid)) {

        if (lockmap(oid).lock.isWriteLockedByCurrentThread){

          lockmap(oid).lock.writeLock().unlock()
          lockmap(oid).transactionIds -= tid
          waitgraph.ch(tid) -= lockmap(oid).owner
        }

        else { //else read unlock

          lockmap(oid).lock.readLock().unlock()
          lockmap(oid).transactionIds -= tid
          waitgraph.ch(tid) -= lockmap(oid).owner

        }


      }

      println("graph after unlock")
      waitgraph.printG()

    }
   def checkCycle(): Boolean ={
     val color = Array.fill (waitgraph.size)(G_N)
     for (v <- color.indices if color(v) == G_N && loopback (v)) return true
     def loopback (u: Int): Boolean =
     {
       if (color(u) == Y_W) return true
       color(u) = Y_W
       for (v <- waitgraph.ch(u) if color(v) != R_D && loopback (v)) return true
       color(u) = R_D
       false
     } // loopback
     false
   }
  }
