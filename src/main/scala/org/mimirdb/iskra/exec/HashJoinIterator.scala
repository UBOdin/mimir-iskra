package org.mimirdb.iskra.exec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import com.typesafe.scalalogging.LazyLogging

case class HashJoinIterator(
  base: QueryResultIterator,
  dimension: QueryResultIterator,
  baseAttribs: Array[(InternalRow) => Any],
  dimensionAttribs: Array[(InternalRow) => Any],
  concatenateBaseOnLeft: Boolean = true
) extends QueryResultIterator
  with LazyLogging
{
  lazy val dimensionIndex: Map[Seq[Any], Seq[InternalRow]] = 
  {
    logger.debug("Building Dimension Index")
    val ret = 
      dimension
        .map { _.copy() } 
        .toSeq
        .groupBy { row => 
          dimensionAttribs.map { _(row) }.toSeq
        }
        .filterKeys { _.forall { _ != null } }
    logger.trace(s"Dimension Table Index:\n ${ret.toSeq.map { x => s"<${x._1.mkString(", ")}> -> ${x._2.map { y => s"<$y>"}.mkString("; ")}" }.mkString("\n")}")
    /* return */ ret
  }
  var baseRow: Option[InternalRow] = None
  var matches: Seq[InternalRow] = Seq.empty
  var matchIdx = 0
  val outputBuffer = new JoinedRow(null, null)

  def getOpt: Option[InternalRow] = 
  {
    logger.trace("Request for next row")
    while(matchIdx >= matches.size || baseRow.isEmpty){
      baseRow = base.getOpt
      logger.trace(s"Finished all matches, advancing to next base row: $baseRow")
      if(baseRow.isEmpty) { return None }
      matchIdx = 0
      matches = dimensionIndex.getOrElse(
        baseAttribs.map { _(baseRow.get) },
        Seq.empty
      )
      logger.trace(s"${matches.size} matches")
    }
    val ret = 
      if(concatenateBaseOnLeft){
        outputBuffer.withLeft(baseRow.get)
                    .withRight(matches(matchIdx))
      } else {
        outputBuffer.withLeft(matches(matchIdx))
                    .withRight(baseRow.get)
      }
    matchIdx = matchIdx + 1
    return Some(ret)
  }

  def reset =
  {
    baseRow = None
    matches = Seq.empty
    matchIdx = 0
    base.reset
  }
}