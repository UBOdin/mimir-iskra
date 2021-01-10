package org.mimirdb.iskra.exec

import org.apache.spark.sql.catalyst.InternalRow

case class UnionIterator(
  sources: Seq[QueryResultIterator]
) extends QueryResultIterator
{
  var idx = 0

  def getOpt: Option[InternalRow] = 
  {
    while(idx < sources.size){
      val buffer = sources(idx).getOpt
      if(buffer.isDefined){ return buffer }
      idx = idx + 1
    }
    return None
  }
  def reset = 
  {
    for(i <- 0 until (idx+1)){
      sources(i).reset
    }
    idx = 0
  }
}