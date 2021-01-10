package org.mimirdb.iskra.exec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import scala.collection.mutable
import com.typesafe.scalalogging.LazyLogging

case class BlockNestedLoopJoinIterator(
  outer: QueryResultIterator,
  inner: QueryResultIterator,
  blockSize: Int = 1000000,
  concatenateOuterOnLeft: Boolean = true
) extends QueryResultIterator
  with LazyLogging
{
  val outerBuffer = mutable.Buffer[InternalRow]()
  var outerIteratorHasMore = true
  var outerBufferIdx = 0
  var innerTuple: Option[InternalRow] = None
  val outputBuffer = new JoinedRow(null, null)
  var firstPass = true

  def refreshOuterBuffer =
  {
    outerBuffer.clear()
    while(outerIteratorHasMore && outerBuffer.size < blockSize){
      logger.trace("Reading next tuple to populate outer block")
      outer.getOpt match {
        case Some(tuple) => outerBuffer.append(tuple)
        case None => outerIteratorHasMore = false
      }
    }
  }

  def getOpt: Option[InternalRow] = 
  {
    // If we've fallen off the edge of the outer buffer, advance it 
    // and restart the inner iterator
    if(firstPass) { 
      firstPass = false
      logger.trace("Prepopulating outer buffer")
      refreshOuterBuffer 
      // if we need to read a tuple, do so (this should only happen on the first pass)
      if(innerTuple.isEmpty){ 
        logger.trace("Prepopulating inner tuple")
        innerTuple = inner.getOpt
        // if there are no more tuples to read (i.e., inner is empty)
        if(innerTuple.isEmpty){
          logger.trace("Inner iterator is empty!")
          return None
        }
      }
    }
    if(outerBufferIdx >= outerBuffer.size){
      logger.trace("Advancing to next inner tuple")
      innerTuple = inner.getOpt
      if(innerTuple.isEmpty){
        logger.trace("Advancing to next outer block")
        refreshOuterBuffer
        if(outerBufferIdx >= outerBuffer.size){ 
          logger.trace("No more data in outer iterator")
          return None 
        }
        inner.reset
        innerTuple = inner.getOpt
      }
      outerBufferIdx = 0

    }
    logger.trace(s"Got join: ${outerBuffer(outerBufferIdx)} x ${innerTuple.get}!")
    // assemble the tuple
    val ret = 
      if(concatenateOuterOnLeft){
        outputBuffer.withLeft(outerBuffer(outerBufferIdx))
                    .withRight(innerTuple.get)
      } else {
        outputBuffer.withLeft(innerTuple.get)
                    .withRight(outerBuffer(outerBufferIdx))
      }
    outerBufferIdx = outerBufferIdx + 1
    return Some(ret)
  }
  def reset = 
  {
    outerBuffer.clear()
    outer.reset()
    inner.reset()
    outerBufferIdx = 0
    innerTuple = None
    firstPass = true
  }
}