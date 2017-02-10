
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller
 *  @version 1.2
 *  @date    Tue Jan 24 14:31:26 EST 2017
 *  @see     LICENSE (MIT style license file).
 */

package trans

import java.nio.ByteBuffer

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.util.control.Breaks._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDB` object represents the Volatile Database.
 */
object VDB
{
    type Record = Array [Byte]                           // record type
    type LogRec = Tuple4 [Int, Int, Record, Record]      // log record type (tid, oid, v_old, v_new)

    private val DEBUG         = true                     // debug flag
    private val pages         = 5                        // number of pages in cache
    private val recs_per_page = 32                       // number of record per page
    private val record_size   = 128                      // size of record in bytes

    private val BEGIN    = -1
    private val COMMIT   = -2
    private val ROLLBACK = -3

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** The `Page` case class 
     */
    case class Page ()
    {
         val p = Array.ofDim [Record] (recs_per_page)
         override def toString = s"Page( + ${p.deep} + )\n" 
    } // page class

            val cache  = Array.ofDim [Page] (pages)      // database cache
            val logBuf = ArrayBuffer [LogRec] ()         // log buffer
    private val map    = Map [Int, Int] ()               // map for finding pages in cache

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Initialize the cache.
     */
    def initCache ()
    {
        for (i <- 0 until pages) {
            val pg = Page ()
            for (j <- 0 until recs_per_page) pg.p(j) = genRecord (i, j)
            cache(i) = pg
            map += i -> i
        } // for
    } // initCache

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid' from the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def read (tid: Int, oid: Int): (Record, Int) =
    {
        if (DEBUG) println (s"read ($tid, $oid)")
        val cpi = map(oid / recs_per_page)         // the cache page index
        val pg = cache(cpi)                        // page in cache
        (pg.p(oid % recs_per_page), cpi)           // record, location in cache
    } // read

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the 'newVal' record to the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def write (tid: Int, oid: Int, newVal: Record)
    {
        if (DEBUG) println (s"write ($tid, $oid, $newVal)")
        val (oldVal, cpage) = read (tid, oid)
        logBuf += ((tid, oid, oldVal, newVal))
        val pg = cache(map(oid / recs_per_page))
        pg.p(oid % recs_per_page) = newVal
    } // write

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Begin the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def begin (tid: Int)
    {
        if (DEBUG) println (s"begin ($tid)")
        logBuf += ((tid, BEGIN, null, null))
    } // begin

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Commit the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def commit (tid: Int)
    {
        if (DEBUG) println (s"commit ($tid)")
        logBuf += ((tid, COMMIT, null, null))
        // I M P L E M E N T
		var start_index=0
		
		for(i <- logBuf.size-1 until 0){// find the previous commit or rollback in logbuffer
		
		val logrec= logBuf(i)
		if (logrec._1==tid && (logrec._2==COMMIT || logrec._2==ROLLBACK) )
			start_index=i
		
		}
		
		
		start_index+=1 // operation after last commit
		
		for(j <- start_index until logBuf.size-1)// write to pdb from start_index to end-1(before last commit)
		{
		
		val logRec=logBuf(j)
		if (logRec._1==tid && (logRec._2!=BEGIN && logRec._2!=COMMIT && logRec._2!=ROLLBACK )){
			
			val oid=logRec._2
			val pg = cache(map(oid / recs_per_page))
			val rec=pg.p(oid % recs_per_page) // value
			PDB.writeLog(logRec)
			PDB.writeStore(rec)
				
		}
		
		if(logRec._1==tid && (logRec._2==BEGIN || logRec._2==COMMIT) )
			PDB.writeLog(logRec)
		
				
		
		
		}
		
		
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def rollback (tid: Int)
    {
        if (DEBUG) println (s"rollback ($tid)")
        logBuf += ((tid, ROLLBACK, null, null))
        // I M P L E M E N T
		for (i <- logBuf.size until 0){
		
		val logRec= logBuf(i)
		if (logRec._1==tid && logRec._2==BEGIN)
			break
		
		if (logRec._1==tid)
			undo(logRec)		
		
		}
    } // rollback
	
	
	def undo(logRec:LogRec)
	{
	
	if (DEBUG) println (s"undo ($logRec._1, $logRec._2, $logRec._3,$logRec._4)")

		val oid= logRec._2
    val pg = cache(map(oid/ recs_per_page))
        pg.p(oid % recs_per_page) = logRec._3

	
	
	}
	
	
	def logRecordtoByte(rec: LogRec) : Array[Byte] = 
	{
		val bytebuffer = ByteBuffer.allocate(264)
		bytebuffer.putInt(rec._1)
		bytebuffer.putInt(rec._2)
		if(rec._3 == null) bytebuffer.put("null".getBytes()) else bytebuffer.put(rec._3)
		if(rec._4 == null) bytebuffer.put("null".getBytes()) else bytebuffer.put(rec._4)
		bytebuffer.array()
	}

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Generate the ith record.
     *  @param i  the page number
     *  @param j  the record number within the page
     */
    def genRecord (i: Int, j: Int): Record = str2record (s"Page $i Record $j ")

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert a string to a record.
     *  @param str  the string to convert 
     */
    def str2record (str: String): Record = (str + "-" * (record_size - str.size)).getBytes

} // VDB


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDBTest` object is used to test the `VDB` object.
 *  > run-main trans.VDBTest
 */
object VDBTest extends App
{
    VDB.initCache ()
    println ("\nPrint cache")
    for (pg <- VDB.cache; rec <- pg.p) println (new String (rec))   // as text
//  for (pg <- VDB.cache; rec <- pg.p) println (rec.deep)           // as byte array
//  for (pg <- VDB.cache; rec <- pg.p) println (rec.size)           // number of bytes

    println ("\nTest reads and writes:")
    println ("read (2, 40)")
    println (new String (VDB.read (2, 40)._1))
    val newVal = VDB.str2record ("new value for record 40 ")
    println (s"write (2, 40, ${new String (newVal)})")
    VDB.write (2, 40, newVal)
    println ("read (2, 40)")
    println (new String (VDB.read (2, 40)._1))
    
    println ("\nPrint cache")
    for (pg <- VDB.cache; rec <- pg.p) println (new String (rec))   // as text

} // VDBTest

