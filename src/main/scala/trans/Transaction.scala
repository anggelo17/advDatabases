
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

    private val transaction_timestamp= System.currentTimeMillis() //trans timestamp

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
     */
    override def run ()
    {
        //begin ()
        for (i <- sch.indices) {
            val op = sch(i)
            println (sch(i))
            if (op._1 == r) {

              if(protocol==TSOproto) {


                val answer = tso.checkTso(transaction_timestamp, op)
                if (answer.equals("OK")) {
                  println(" permission to read granted")
                  //read(op._3)
              }
                else rollback()
              }


            }

            else {// write

              if(protocol==TSOproto) {


                val answer = tso.checkTso(transaction_timestamp, op)
                if (answer.equals("OK")) {
                  println(" permission to write granted")
                  //write(op._3, VDB.str2record(op.toString))
                }
                else rollback()
              }


            }//else
        } // for

      println(" commiting")
        //commit ()
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
        VDB.commit (tid)
        if (DEBUG) println (VDB.logBuf)
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback this transaction.
     */
    def rollback ()
    {
        VDB.rollback (tid)
    } // rollback

} // Transaction class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
 *  > run-main trans.TransactionTest
 */
object TransactionTest extends App
{

  val tso=Tso(0,0)

    val t1 = new Transaction (new Schedule (List ( (r, 0, 0), (r, 0, 1), (w, 0, 0), (w, 0, 1),(r,0,0),(r,0,1) )),1,tso)  // last 1 is for the tso ; 2 for 2pl
   //val t2 = new Transaction (new Schedule (List ( (r, 1, 0), (r, 1, 1), (w, 1, 0), (w, 1, 1) )),1,tso) // last 1 is for the tso ; 2 for 2pl

    t1.start ()
   // t2.start ()



} // TransactionTest object

