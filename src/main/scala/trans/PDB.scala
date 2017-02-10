package trans


import java.io.RandomAccessFile
import java.lang.Thread
import java.nio.ByteBuffer

import scala.collection.mutable.{ArrayBuffer, Map}


class CheckPoint () extends Thread
{
    private val DEBUG = true              // debug flag
     private val CHECKPOINT    = -4

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this checkpoint by executing its operations.
     */
    override def run ()
    {
		 if (DEBUG) println ("introducing a CHECKPOINT")
        val record= (CHECKPOINT, CHECKPOINT, null, null)
        val position=PDB.log.length()
        PDB.log.seek(position)
        PDB.writeLog(record)
    } // run
	
	def sleep(){
	
	Thread.sleep(2000)
	
	}
	
	
}


class Recover() extends Thread
{
    private val DEBUG = true              // debug flag
	 private val CHECKPOINT    = -4
	 
	 
	  private val BEGIN    = -1
    private val COMMIT   = -2
    private val ROLLBACK = -3
	
	val activeList=ArrayBuffer[Int]()// tid activeList
	val commitList=ArrayBuffer[Int]()
	val uncommitedList=ArrayBuffer[Int]()


  type Record = Array [Byte]                           // record type
type LogRec = Tuple4 [Int, Int, Record, Record]





  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

  def fromBytestoRecord(bytes: Array[Byte]) : LogRec={

    val bytebuffer=ByteBuffer.wrap(bytes)

    val tid=bytebuffer.getInt
    val oid=bytebuffer.getInt
    val old=(for (i <- 0 until 128) yield bytebuffer.get()).toArray
    val newv =(for (i <- 0 until 128) yield bytebuffer.get()).toArray

    val logRecord=new LogRec(tid,oid,old,newv)

    logRecord

  }

  /** Run this recover by executing its operations.
     */
    override def run () //recover
    {
        if (DEBUG) println ("start recovering")
		val log=PDB.log
		var checkpoint_index=0

      val logBuf = ArrayBuffer [LogRec] ()

      var position=0
      log.seek(0)

      var eof=0

      while(eof!= -1){ //read from log RAF and store in array

        val bytes=Array.ofDim[Byte](264)
        eof=log.read(bytes)

        if(eof!= -1) {

          val entryRecord=fromBytestoRecord(bytes)
          logBuf+= entryRecord
            position +=264
            log.seek(position)

          }

        }




      for(i <- logBuf.size until 0)
			  if( logBuf(i)._2==CHECKPOINT)
				  checkpoint_index=i
				
				
				//PHASE 1
				
		for(j <- checkpoint_index-1 until 0)
		{ //filling the activeList
		
		var found=false
		
		if(logBuf(j)._2==BEGIN)
			{
			for( z <- j+1 until checkpoint_index)
				if (logBuf(z)._2==COMMIT)
					found=true
			
					
			if (found==false)
				activeList+=logBuf(j)._1
		
			}
		
		
		}
		var start_index=0
		
		for(i <- checkpoint_index-1 until 0){ //move back until all begin record of active transac arre seen
		
			for(j <- 0 until activeList.size)
				if( logBuf(i)._1== activeList(j))
					start_index=i
		
		}
		
		for(k <- start_index until logBuf.size)
		{ //check commit,begin and write:
		
			if(logBuf(k)._2==BEGIN)
			{//check active list
			
			}
			
			
			if (logBuf(k)._2==COMMIT)
			{
			if (k>checkpoint_index)
				commitList+=logBuf(k)._1   //add tid
			
			
			
			}
		
			if (logBuf(k)._2!=BEGIN && logBuf(k)._2!=COMMIT && logBuf(k)._2!=CHECKPOINT)// write operation
			{
			
				if (commitList contains logBuf(k)._1){
				
				}
				else{ // not in commitList
				
					uncommitedList+=logBuf(k)._1
					//undo(log(k))
					}
			
			
			
			}
		
		}
		
		
		//phase 2:
		for(k <- start_index until logBuf.size)
		{ //check write operation
		
			if(logBuf(k)._2>=0)// write operation
			{
				if(commitList contains logBuf(k)._1)
					{
					//redo(log(k))
					}
			
			}
		
		
		}
		
		
		
		
				
    } // run
	
	
	def sleep(){
	
	Thread.sleep(5000)
	
	}
	
	
}

object PDB
{

 type Record = Array [Byte]                           // record type
 type LogRec = Tuple4 [Int, Int, Record, Record]         // log record type (tid, oid, v_old, v_new)

val store = new RandomAccessFile("store.txt","rw")

 val log = new RandomAccessFile("log.txt","rw")


def writeStore(bytes:Array[Byte])
{

val position=store.length()
store.seek(position)
store.write(bytes)
store.close()



}


def writeLog(logRec:LogRec)
{
  val position=log.length()
  log.seek(position)
  log.write(logRecordtoByte(logRec))
	log.close()



}

	def logRecordtoByte(rec: LogRec) : Array[Byte] = {
		val bytebuffer = ByteBuffer.allocate(264)
		bytebuffer.putInt(rec._1)
		bytebuffer.putInt(rec._2)
		if(rec._3 == null) bytebuffer.put("null".getBytes()) else bytebuffer.put(rec._3)
		if(rec._4 == null) bytebuffer.put("null".getBytes()) else bytebuffer.put(rec._4)
		bytebuffer.array()
	}


}

object PDBTest extends App
{


		
		while(true){

      val checkpointThread=new CheckPoint()
      val recoverThread=new Recover()

      if(!checkpointThread.isAlive())
		      checkpointThread.start()
		Thread.sleep(5000)

      if(!recoverThread.isAlive())
		      recoverThread.start()
    Thread.sleep(2000)
		
		
		
		
		}




}