package org.mimirdb.iskra

import java.io.File
import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.catalyst.plans.logical._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow

import org.mimirdb.iskra.exec._

object Compiler
  extends Object
  with LazyLogging
{
  def apply(df: DataFrame): Seq[Row] = 
    apply(df.queryExecution.analyzed, df.queryExecution.sparkSession)

  def apply(plan: LogicalPlan, spark: SparkSession): Seq[Row] = 
  {
    val schema = plan.schema
    return compile(plan, spark)
                .map { irow => new GenericRowWithSchema(irow.toSeq(plan.schema).toArray, schema) }
                .toIndexedSeq
  }

  def compile(plan: LogicalPlan, spark: SparkSession): QueryResultIterator = 
  {
    def FallBackToSpark = {
      logger.warn(s"Unsupported operator: ${plan.getClass}.  Falling back to spark")
      TableSource(plan, spark)
    }
    def RecurTo(subplan: LogicalPlan) = 
      compile(subplan, spark)
    def CompileExpr(expr: Expression, subplan: LogicalPlan): (InternalRow => Any) = 
      compile(expr, subplan.output).eval(_)

    plan match {
      case _:ReturnAnswer => ???
      case _:Subquery => ???
      case Project(exprs, subplan) => 
        ProjectIterator(exprs.map { CompileExpr(_, subplan) }, RecurTo(subplan))
      case _:Generate => ???
      case _:Filter => ???
      case _:Intersect => ???
      case _:Except => ???
      case _:Union => ???
      case _:Join => ???
      case _:InsertIntoDir => ???
      case _:View => ???
      case _:With => ???
      case _:WithWindowDefinition => ???
      case _:Sort => ???
      case _:Range => ???
      case _:Aggregate => ???
      case _:Window => ???
      case _:Expand => ???
      case _:GroupingSets => ???
      case _:Pivot => ???
      case _:GlobalLimit => ???
      case _:LocalLimit => ???
      case _:Tail => ???
      case _:SubqueryAlias => ???
      case _:Sample => ???
      case _:Distinct => ???
      case _:Repartition => ???
      case _:RepartitionByExpression => ???
      case _:OneRowRelation => ???
      case _:Deduplicate => ???
      case _:CollectMetrics => ???
      case _:LocalRelation => ???
      case LogicalRelation(relation, output, catalogTable, isStreaming) => {
        relation match {
          case HadoopFsRelation(file: InMemoryFileIndex, partitionSchema, dataSchema, bucketSpec, fileFormat, options) => {
            val rootPaths = file.rootPaths
            if((rootPaths.size == 1) && rootPaths(0).toUri.getScheme.equals("file")){
              TableSource(
                new File(rootPaths(0).toUri().getPath()), 
                dataSchema,
                fileFormat.buildReaderWithPartitionValues(
                  sparkSession = spark,
                  dataSchema = dataSchema,
                  partitionSchema = partitionSchema,
                  requiredSchema = dataSchema,
                  filters = Seq.empty,
                  options = options,
                  hadoopConf = spark.sessionState.newHadoopConf()
                )
              )
            } else {
              FallBackToSpark
            }
          }
          case _ => FallBackToSpark
        }
      }
      case l:LeafNode => FallBackToSpark
    }
  }

  def compile(expr: Expression, inputs: Seq[Attribute]): Expression = 
  {
    val inputPosition = inputs.map { _.exprId }.zipWithIndex.toMap

    expr.transformDown { 
      case a:AttributeReference => 
        BoundReference(inputPosition(a.exprId), a.dataType, a.nullable)
    }
  }
}