
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

import java.io.{BufferedWriter, FileWriter, Writer}

import scala.util.control.Breaks._
import Operation._
import breeze.linalg._
import breeze.numerics._
import breeze.polynomial
import breeze.plot._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object TransactionStats {
    var start: Long = System.currentTimeMillis
    var end: Long = 0
    var count: Int = 0 // successful transactions
    var rolls: Int = 0

  //var volatile la=0

}

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` companion object
 */
object Transaction
{
    private var count = -1

    def nextCount () = { count += 1; count }


    VDB.initCache ()

} // Transaction object

import Transaction._
import TransactionStats._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
 *  @param sch  the schedule/order of operations for this transaction.
 */
class Transaction (sch: Schedule,protocol:Int,tso:Tso,var scheduleBuffer:ArrayBuffer[Op]) extends Thread
{
    private val DEBUG = true              // debug flag
    private val tid = nextCount ()        // tarnsaction identifier

    val TSOproto = 1
    val TwoPL = 2

   // private val transaction_timestamp= System.currentTimeMillis() //trans timestamp

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
     */
    override def run ()
    {

      var rollbackb=false
        //begin ()
      val size=sch.sizeSchedule
      var i=0;

        while (i < size) {
            val op = sch(i)
            println (sch(i))
            if (op._1 == r) {

              if(protocol==TSOproto && !rollbackb) {


                val answer = tso.checkTso(System.currentTimeMillis(), op)
                if (answer.equals("OK")) {


                  scheduleBuffer.synchronized{
                    scheduleBuffer+=op
                  }
                 // scheduleBuffer += sch(i)
                //  for(k<-0 until  TransactionStats.scheduleBuffer.size)
                    println("op "+scheduleBuffer.size)
                  println(" permission to read granted")
                  //read(op._3)
                  println("adding "+op)

              }
                else {
                  rollback()
                  rollbackb=true
                }
              }


            }

            else {// write

              if(protocol==TSOproto && !rollbackb) {


                val answer = tso.checkTso(System.currentTimeMillis(), op)
                if (answer.equals("OK")) {

                  scheduleBuffer.synchronized {
                  scheduleBuffer += sch(i)
                }
                  println(" permission to write granted")
                  //for(k<-0 until  TransactionStats.scheduleBuffer.size)
                    println("op "+scheduleBuffer.size)
                  //write(op._3, VDB.str2record(op.toString))
                 // TransactionStats.scheduleBuffer+=op
                  println("adding "+op)
                }
                else {
                  rollback()
                  rollbackb=true
                }
              }


            }//else

          if (rollbackb){
            i=0 // start again the loop
            rollbackb=false
          }
          else i+=1 // next op

        } // while

        if(!rollbackb)
            commit ()
    } // run

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
     *  @param oid  the object/record being read
     */
    def read (oid: Int): VDB.Record =
    {
        VDB.read (tid, oid)._1
    } // read

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
     *  @param oid    the object/record being written
     *  @param value  the new value for the the record
     */
    def write (oid: Int, value: VDB.Record)
    {
        VDB.write (tid, oid, value)
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
      println(" commiting")
        TransactionStats.count+=1
      //  VDB.commit (tid)
        //if (DEBUG) println (VDB.logBuf)
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback this transaction.
     */
    def rollback ()
    {
        println("rolling back")
        TransactionStats.rolls+=1
       // VDB.rollback (tid)

    } // rollback

} // Transaction class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
 *  > run-main trans.TransactionTest
 */
object TransactionTest extends App
{

  val tso=Tso(0,0)

  val scheduleBuffer=new ArrayBuffer[Op]()


  val startTime=System.currentTimeMillis()

  val NUMBER_THREADS=55

  var j,i=0

  //for(i<-0 until NUMBER_THREADS) { // transactions*2----change this number to test the concurrent number of transactions

    //j=i*2

    var nTrans = 2
    var nOps = 2
    var nObjs = 32

    val t1 = new Transaction(new Schedule(List((r, j, 0), (r, j, 1),(w,0,1) )), 1, tso,scheduleBuffer)
  val t2= new  Transaction(Schedule.gen(1,nOps,nObjs),1,tso,scheduleBuffer)
  val t3= new  Transaction(Schedule.gen(2,nOps,nObjs),1,tso,scheduleBuffer)

  t1.start()
    t2.start()
  t3.start()
   t1.join()
    t2.join()
  t3.join()


  //}

 // Thread sleep 15000
println(scheduleBuffer.size)


  for(k<-0 until  scheduleBuffer.size)
    println("op "+scheduleBuffer(k))

//
//  var nTrans = 100
//  var nOps = 2
//  var nObjs = 32
//  //val t1 = new Transaction (new Schedule (List ( (r, 0, 0), (r, 0, 1),(w, 0, 0), (w, 0, 1))),2)
//  //val t2 = new Transaction (new Schedule (List ( (r, 1, 0), (r, 1, 1), (w, 1, 0), (w, 1, 1) )),2)
//  //t1.start ()
//  //t2.start ()
//  var transactions = Array.ofDim[Transaction](nTrans)
//  for(i<-transactions.indices)
//  {
//    transactions(i)=new Transaction(Schedule.gen(i,nOps,nObjs),2,tso)
//    transactions(i).start()
//    //transactions.jo
//  }
//
//  for(i<-transactions.indices)
//  {
//    transactions(i).join()
//  }
//
//  println("all threads finished...")
//
//  val endTime=System.currentTimeMillis()
//
//  var totalTime=endTime-startTime
//
//  println(" total time= "+totalTime)
//
//  println("STATS:")
//  println("committed Transactions ="+ TransactionStats.count)
//  println("rollback Transactions ="+TransactionStats.rolls)
//
//  val dtotalTime=totalTime.toDouble/1000.0
//  println(" total time= "+dtotalTime)
//
// val tps=(TransactionStats.count.toDouble/ dtotalTime )
//  println("Transactions per second= "+tps)
//
//
//  val output = new BufferedWriter(new FileWriter("tso.txt",true));  //clears file every time
//  output.append(tps+","+nTrans+"\n");
//  output.close();
//
//  val throughput = DenseVector( Source.fromFile("tso.txt")
//    .getLines.map(_.split(",")(0).toDouble).toSeq :_ * )
//
//  for(i<-throughput)
//    println(i)
//
//  val threads = DenseVector( Source.fromFile("tso.txt")
//    .getLines.map(_.split(",")(1).toDouble).toSeq :_ * )
//
//  for(i<-threads)
//    println(i)
//
//  val fig = Figure()
//  val plt = fig.subplot(0)
//  plt += plot(threads , throughput)
//  plt.xlabel=" Threads"
//  plt.ylabel=" Transactions/second"
//
//  fig.refresh()
//




} // TransactionTest object

