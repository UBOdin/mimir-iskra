package org.mimirdb.iskra.exec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.Row

case class ProjectIterator(
  exprs: Seq[(InternalRow => Any)], 
  source: QueryResultIterator
) extends QueryResultIterator
{
  def getOpt: Option[InternalRow] = 
    source.getOpt.map { row => 
      InternalRow.fromSeq(exprs.map { _(row) })
    }
  def reset = source.reset
}