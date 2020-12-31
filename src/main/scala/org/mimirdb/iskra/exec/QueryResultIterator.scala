package org.mimirdb.iskra.exec

import org.apache.spark.sql.catalyst.InternalRow

trait QueryResultIterator extends Iterator[InternalRow]
{
  var buffer: Option[InternalRow] = None

  def getOpt: Option[InternalRow]
  def reset(): Unit

  def next: InternalRow = {
    if(buffer.isEmpty){ buffer = getOpt }
    if(buffer.isDefined){
      val ret = buffer.get
      buffer = None
      return ret
    } else {
      return null
    }
  }

  def hasNext: Boolean = 
  {
    if(buffer.isEmpty){ buffer = getOpt }
    return buffer.isDefined
  }

}