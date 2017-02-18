package trans

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import trans.TrafficLight._
/**
  * Created by supriya on 2/9/17.
  */
object LockTable {
class Lock(tid: Int) {
  val lock = new ReentrantReadWriteLock()
  var owner=tid
}
  var lockmap = new mutable.HashMap[Int,Lock]()
  val nTrans = 2
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
    if(wait) {

      println("adding an edge from.."+tid+" to "+ownerTid+" when rl on"+oid)
      waitgraph.ch(tid) += ownerTid}

    if(LockTable.checkCycle())
      {
        trans.rollbackid = true
        if(wait) waitgraph.ch(tid) -= ownerTid
      }
    lockmap(oid).lock.readLock().lock() //tid got the readlock
    println("after ..read locks on " + oid + " : " + lockmap(oid).lock.getReadLockCount + " " + lockmap(oid).lock.getReadHoldCount)


    //println("======="+lockmap(oid).lock.toString)
  }
  def wl(oid: Int,tid: Int,trans: Transaction) = {
    var wait = false
    var ownerTid = -1


    println("starting wl on "+oid)

    if (!lockmap.keySet.contains(oid)) {
      println("inserting.."+oid)
      lockmap += (oid) -> new Lock(tid)
    }



    if(lockmap(oid).owner != tid)
      wait = true

    ownerTid = lockmap(oid).owner
    //println( " : " + lockmap(oid).lock.getReadLockCount + " " + lockmap(oid).lock.getReadHoldCount)
    if(wait)
      {
        println("adding an edge from.."+tid+" to "+ownerTid+" when wl on"+oid)
        waitgraph.ch(tid) += ownerTid
      }
    if(LockTable.checkCycle())
      {
        println("there is a cycle????")
        waitgraph.printG()
        trans.rollbackid=true
        if(wait)
        waitgraph.ch(tid) -= ownerTid
      }



      val readLockCount = lockmap(oid).lock.getReadLockCount
      for (i <- 0 until readLockCount) {

        println("ulocking.."+oid)

        lockmap(oid).lock.readLock().unlock()
      }



    lockmap(oid).lock.writeLock().lock()
    //println("======="+lockmap(oid).lock.toString)
  }
    def ul(op: Char,oid: Int,tid:Int): Unit = {
      if(lockmap.keySet.contains(oid)) {
        if (op=='w') {
          println(s"unlocking writelock on $op, $oid")

          lockmap(oid).lock.writeLock().synchronized {
            waitgraph.ch(tid) -= lockmap(oid).owner
            lockmap(oid).lock.writeLock().unlock()
        }

        }
       /* else if(TransactionStats.upgradelock)
          {
            lockmap(oid).lock.writeLock().unlock()
            TransactionStats.upgradelock=false
          } */
        else {
         // println(s"unlocking readlock on $op, $oid")

          waitgraph.ch(tid) -= lockmap(oid).owner
          val readLockCount = lockmap(oid).lock.getReadLockCount
          for (i <- 0 until readLockCount) {

            println("unlocking readlock in ul.."+oid)

            lockmap(oid).lock.readLock().unlock()
          }

         // lockmap(oid).lock.readLock().unlock()

        }
      }
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
