
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

import Operation._

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

    do {
      begin()
      val size = sch.sizeSchedule
      var count = 0;
      for (i <- sch) {
        val op = i
        val oid = i._3
        val tid = i._2
        println(i)
        if (op._1 == r) {
          if (protocol == twoPl) {
            if (searchlock(oid, tid, sch,count)) {
              wl(oid, op._2, this)
              TransactionStats.upgradelock=true
              println(s"got upgraded writelock on $oid")
             // read(oid)
            }
            else {
              rl(op._3, op._2, this)
              println(s"got readlock on $op")
              //read(op._3)
              // lockmap(oid).lock.readLock().unlock() // ul(op._3)
            }
          } //if
        } //if r/w
        else {
          if (protocol == twoPl) {
            wl(op._3, op._2, this)
            println(s"got writelock on $oid")
            //write(op._3, VDB.str2record(op.toString))
          }
        } //else of r/w
        count += 1
      } // for
      commit()
    } while(rollbackid)
  } // run


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Read the record with the given 'oid'.
    *  @param oid  the object/record being read
    */
  def read (oid: Int): VDB.Record =
  {
    //obtain lock. see if there is a write ahead, so the lock should be upgraded - searchlock
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
    VDB.write (tid, oid, value)
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
  def commit ()
  {
    for(i <- sch)
      {
        if(i._1==w)
        ul(w,i._3)
        else
          ul(r,i._3)
      }
   TransactionStats.count += 1
    //VDB.commit (tid)
   // if (DEBUG) println (VDB.logBuf)
  } // commit

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** Rollback this transaction.
    */
  def rollback ()
  {
    VDB.rollback (tid)
  } // rollback
def searchlock(oid:Int, tid: Int,s:Schedule, index: Int) : Boolean =
{
  var flag=false
  for(i <- index+1 until s.sizeSchedule) {
    if (oid == s(i)._3 && tid == s(i)._2 && s(i)._1 == 'w')
    flag=true
    println(s"in searchlock $i $flag")
}
  flag
}//to see if there is a writelock ahead
} // Transaction class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
  *  > run-main trans.TransactionTest
  */
object TransactionTest extends App
{
 // val t1=new Transaction(new Schedule(List((r,0,0))),2)//(r,0,1),(w, 0, 0), (w, 0, 1))),2)
  val startTime = System.currentTimeMillis()
  var nTrans = 3
  var nOps = 2
  var nObjs = 3
  /*val t1 = new Transaction (new Schedule (List ( (r, 0, 0), (r, 0, 1),(w, 0, 0), (w, 0, 1))),2)
  val t2 = new Transaction (new Schedule (List ( (w, 1, 0), (r, 1, 1), (w, 1, 0), (w, 1, 1) )),2)
  t1.start ()
  t2.start ()
  */
  var transactions = Array.ofDim[Transaction](nTrans)
  var j=0

 /* for(i<-0 until 5) { // transactions*2----change this number to test the concurrent number of transactions

    j=i*2

    val t1 = new Transaction(new Schedule(List((r, j, 0), (r, j, 1), (w, j, 0), (w, j, 1), (r, j, 0), (r, j, 1))), 2)
    // last 1 is for the tso ; 2 for 2pl
    val t2 = new Transaction(new Schedule(List((r, j+1, 0), (r,j+1, 1), (w, j+1, 0), (w, j+1, 1))), 2) // last 1 is for the tso ; 2 for 2pl

    t1.start()
    t2.start()

  }
*/
   for(i<-transactions.indices)
     {
       transactions(i)=new Transaction(Schedule.gen(i,nOps,nObjs),2)
       transactions(i).start()
     }
  Thread sleep 10000
  val endTime = System.currentTimeMillis()
  val totalTime=endTime-startTime
  println(TransactionStats.count)
  println(s"totaltime=$totalTime")
  val tps =TransactionStats.count/(totalTime/1000)
  println(s"tps=$tps")
} // TransactionTest object
