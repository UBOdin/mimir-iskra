package org.mimirdb.iskra.exec

import org.apache.spark.sql.catalyst.InternalRow

case class FilterIterator(
  condition: (InternalRow => Boolean), 
  source: QueryResultIterator
) extends QueryResultIterator
{
  def getOpt: Option[InternalRow] = 
  {
    var buffer = source.getOpt
    while(buffer != None){
      if( condition(buffer.get) ){ return buffer }
      buffer = source.getOpt
    }
    return None
  }
  def reset = source.reset
}