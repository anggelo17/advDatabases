
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller
  *  @version 1.1
  *  @date    Tue Jan 10 14:34:43 EST 2017
  *  @see     LICENSE (MIT style license file).
  *------------------------------------------------------------------------------
  *  Instructions:
  *      Download sbt
  *      Download transactions.zip
  *      unzip transactions.zip
  *      cd transactions
  *      sbt
  *      > compile
  *      > run-main trans.ScheduleTest
  *      > exit
  */

package trans

import java.util.concurrent.locks.ReentrantReadWriteLock

import Operation._

import scala.collection.mutable.ArrayBuffer

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` companion object
  */

object TransactionStats {
  var start: Long = System.currentTimeMillis
  var end: Long = 0
  var count: Int = 0 // successful transactions
  var rolls: Int = 0
  var upgradelock = false
}
object Transaction
{
  private var count = -1

  def nextCount () = { count += 1; count }


  VDB.initCache ()

} // Transaction object

class OperationLock(Oper:Op,st:String)
{
  val ope=Oper
  val str=st// 3-tuple (r/w, tid, oid)


}

import Transaction._
import LockTable._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
  *  @param sch  the schedule/order of operations for this transaction.
  */
class Transaction (sch: Schedule,protocol: Int) extends Thread
{
  var rollbackid =false
  private val DEBUG = true              // debug flag
private val tid = nextCount ()        // tarnsaction identifier
  val twoPl=2

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Run this transaction by executing its operations.
    */
  override def run ()
  {


      val oidList = new ArrayBuffer[OperationLock]()
      begin()
      val size = sch.sizeSchedule
      var i = 0;
      var result=0;

    if (protocol == twoPl) {

      while (i < size) {// phase 1
        val op = sch(i)

        if (!containsOpera(oidList,op._3)) {

          if (op._1 == r) {// if read

            if (searchlock(op._3, op._2, sch, i)) {
              result=wl(op._3, op._2, this)
              if(result==0) {
                println(s"got writelock on $op")
                val opelock = new OperationLock(op, "wl")
                oidList += opelock
              }
            }
            else {
              result=rl(op._3, op._2, this)
              if(result==0) {
                println(s"got readlock on $op")
                val opelock = new OperationLock(op, "rl")
                oidList += opelock
              }
            }


          }
          else {

            result=wl(op._3, op._2, this)
            if(result==0) {
              println(s"got writelock on $op")
              val opelock = new OperationLock(op, "wl")
              oidList += opelock
            }
            //write(op._3, VDB.str2record(op.toString))

          }//else

        }//if

        if (rollbackid) {
          i = 0;
          rollbackid = false
          rollback(oidList)
        }
        else i += 1

      } // while



      i=0
      while(i<size){
        val ope=sch(i)

        if (ope._1==r)
          read(ope._3)
        else
          write(ope._3,VDB.str2record(ope.toString))

        i += 1
      }

      commit(oidList)

    }//if
  } // run


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Read the record with the given 'oid'.
    *  @param oid  the object/record being read
    */
  def read (oid: Int): VDB.Record =
  {
    //obtain lock. see if there is a write ahead, so the lock should be upgraded - searchlock
    println("reading .."+oid)
    VDB.read (tid, oid)._1
  } // read

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Write the record with the given 'oid'.
    *  @param oid    the object/record being written
    *  @param value  the new value for the the record
    */
  def write (oid: Int, value: VDB.Record)
  {
    //obtain lock
    println("writing .."+oid)
    //VDB.write (tid, oid, value)
    //unlock before commit, so it should be implemented in commit
  } // write

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Begin this transaction.
    */
  def begin ()
  {
    VDB.begin (tid)
  } // begin

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Commit this transaction.
    */
  def commit (oidList:ArrayBuffer[OperationLock])
  {
    var i=0
    while (i < oidList.size)
    {
      if( oidList(i).str=="wl")
        ul(w,oidList(i).ope._3,oidList(i).ope._2)
      else
        ul(r,oidList(i).ope._3,oidList(i).ope._2)

      i += 1
    }

    TransactionStats.count += 1
    //VDB.commit (tid)
   // if (DEBUG) println (VDB.logBuf)
  } // commit

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Rollback this transaction.
    */
  def rollback (oidList:ArrayBuffer[OperationLock])
  {

    println("rolling back")
    for(i <- oidList.indices)
    {
      if( oidList(i).str=="wl")
        ul(w,oidList(i).ope._3,oidList(i).ope._2)
      else
        ul(r,oidList(i).ope._3,oidList(i).ope._2)
    }

    TransactionStats.rolls +=1
    //VDB.rollback (tid)
  } // rollback
def searchlock(oid:Int, tid: Int,s:Schedule, index: Int) : Boolean =
{
  var flag=false
  for(i <- index+1 until s.sizeSchedule) {
    if (oid == s(i)._3 && s(i)._1 == 'w')
    flag=true
    println(s"in searchlock $i $flag")
}
  flag
}//to see if there is a writelock ahead

  def containsOpera(lst:ArrayBuffer[OperationLock],oid:Int):Boolean=
  {
    var f=false

    for( i <- lst.indices){
      var op= lst(i).ope
      if(op._3==oid && lst(i).str=="wl")
        return true

    }
    f
  }

} // Transaction class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
  *  > run-main trans.TransactionTest
  */
object TransactionTest extends App
{

//  val lock = new ReentrantReadWriteLock();
//  lock.readLock().lock();
//
//  // In real code we would go call other methods that end up calling back and
//  // thus locking again
//  lock.readLock().lock();
//  println(lock.getReadHoldCount+" "+lock.getReadLockCount)
//
//  // Now we do some stuff and realise we need to write so try to escalate the
//  // lock as per the Javadocs and the above description
//  lock.readLock().unlock(); // Does not actually release the lock
//  println(lock.getReadHoldCount+" "+lock.getReadLockCount)
//  lock.writeLock().lock();  // Blocks as some thread (this one!) holds read lock
//
//  System.out.println("Will never get here");

  val startTime = System.currentTimeMillis()
  var nTrans = 5
  var nOps = 4
  var nObjs = 10
  /*val t1 = new Transaction (new Schedule (List ( (r, 0, 0), (r, 0, 1),(w, 0, 0), (w, 0, 1))),2)
  val t2 = new Transaction (new Schedule (List ( (w, 1, 0), (r, 1, 1), (w, 1, 0), (w, 1, 1) )),2)
  t1.start ()
  t2.start ()
  */
  var transactions = Array.ofDim[Transaction](nTrans)


//  var j,i=0
//
//    val t1 = new Transaction(new Schedule(List( (r, j, 1), (r, j, 1), (r, j, 0), (w, j, 0),(r,0,0))), 2)
//    // last 1 is for the tso ; 2 for 2pl
//    val t2 = new Transaction(new Schedule(List((w, j+1, 1), (r,j+1, 1), (w, j+1, 0), (w, j+1, 1))), 2) // last 1 is for the tso ; 2 for 2pl
//
//    t1.start()
//    t2.start()



   for(i<-transactions.indices)
     {
       transactions(i)=new Transaction(Schedule.gen(i,nOps,nObjs),2)
       transactions(i).start()
     }


//  Thread sleep 10000
//  val endTime = System.currentTimeMillis()
//  val totalTime=endTime-startTime
//  println("Committed="+ TransactionStats.count)
//  println(s"totaltime=$totalTime")
//  val tps =TransactionStats.count/(totalTime/1000)
//  println(s"tps=$tps")
} // TransactionTest object
