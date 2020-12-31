package org.mimirdb.iskra.exec

import java.io.File
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import com.typesafe.scalalogging.LazyLogging
import org.mimirdb.iskra.util.Timing

class TableSource(getRows: => IndexedSeq[InternalRow], label: String = "Source Table")
  extends QueryResultIterator 
  with LazyLogging
  with Timing
{
  val allRows: IndexedSeq[InternalRow] = 
    time(label+":GetRows") { getRows }
  var idx = 0

  def getOpt =
    if(idx >= allRows.size) { None }
    else { idx = idx + 1; Some(allRows(idx-1)) }

  def reset = { idx = 0 }

  override def toSeq = (
    if(idx == 0) { allRows.toSeq } 
    else { allRows.toSeq.drop(idx) }
  )

}

object TableSource
{
  def apply(plan: LogicalPlan, spark: SparkSession): TableSource = 
    new TableSource({ 
      new DataFrame(spark, plan, RowEncoder(plan.schema))
            .collect() 
            .map { row => InternalRow.fromSeq(row.toSeq) }
    })

  def apply(file: File, schema: StructType, reader: (PartitionedFile) => Iterator[InternalRow]): TableSource = 
  {
    val pfile = PartitionedFile(
      partitionValues = InternalRow(),
      filePath = file.toString(),
      start = 0,
      length = file.length()
    )
    new TableSource(
      { reader(pfile).map { row => row.copy() }.toIndexedSeq },
      label = file.toString()
    )
  }

  def oneRow: TableSource = 
    new TableSource({ IndexedSeq(InternalRow()) })
}