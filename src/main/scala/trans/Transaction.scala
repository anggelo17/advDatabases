
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

import scala.util.control.Breaks._

import Operation._


object TransactionStats {
    var start: Long = System.currentTimeMillis
    var end: Long = 0
    var count: Int = 0 // successful transactions
    var rolls: Int = 0
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

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
 *  @param sch  the schedule/order of operations for this transaction.
 */
class Transaction (sch: Schedule,protocol:Int,tso:Tso) extends Thread
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

      var stopb=false
        //begin ()
        for (i <- sch.indices) {
            val op = sch(i)
            println (sch(i))
            if (op._1 == r) {

              if(protocol==TSOproto && !stopb) {


                val answer = tso.checkTso(System.currentTimeMillis(), op)
                if (answer.equals("OK")) {
                  println(" permission to read granted")
                  //read(op._3)
              }
                else {
                  rollback()
                  stopb=true
                }
              }


            }

            else {// write

              if(protocol==TSOproto && !stopb) {


                val answer = tso.checkTso(System.currentTimeMillis(), op)
                if (answer.equals("OK")) {
                  println(" permission to write granted")
                  //write(op._3, VDB.str2record(op.toString))
                }
                else {
                  rollback()
                  stopb=true
                }
              }


            }//else
        } // for

        if(!stopb)
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


  val startTime=System.currentTimeMillis()

  var j=0

  for(i<-0 until 5) { // transactions*2----change this number to test the concurrent number of transactions

    j=i*2

    val t1 = new Transaction(new Schedule(List((r, j, 0), (r, j, 1), (w, j, 0), (w, j, 1), (r, j, 0), (r, j, 1))), 1, tso)
    // last 1 is for the tso ; 2 for 2pl
    val t2 = new Transaction(new Schedule(List((r, j+1, 0), (r,j+1, 1), (w, j+1, 0), (w, j+1, 1))), 1, tso) // last 1 is for the tso ; 2 for 2pl

    t1.start()
   t2.start()

  }


  Thread sleep 10000

  println("all threads finished...")

  val endTime=System.currentTimeMillis()
  val totalTime=endTime-startTime

  println("STATS:")
  println("committed Transactions ="+ TransactionStats.count)
  println("rollback Transactions ="+TransactionStats.rolls)

 val tps=(TransactionStats.count.toDouble/ (totalTime/1000) )
  println("Transactions per second= "+tps)




} // TransactionTest object

