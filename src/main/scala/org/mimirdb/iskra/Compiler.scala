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
import org.apache.spark.sql.types._

import org.mimirdb.iskra.exec._
import org.apache.spark.sql.catalyst.plans.{Inner => InnerJoin }

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

  def compareOutputs(outputA: Seq[Attribute], outputB: Seq[Attribute]) =
    outputA.zip(outputB).forall{ case (a, b) => a.name.equalsIgnoreCase(b.name) && a.dataType.equals(b.dataType) }

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
      case Project(exprs, subplan) => 
        ProjectIterator(exprs.map { CompileExpr(_, subplan) }, RecurTo(subplan))
      case Filter(condition, subplan) => 
      {
        assert(condition.dataType.equals(BooleanType))
        val eval = CompileExpr(condition, subplan)
        FilterIterator( 
          eval(_).asInstanceOf[Boolean], 
          RecurTo(subplan)
        )
      }
      case Union(subplans) => 
      {
        assert(!subplans.isEmpty, "Union with no inputs")
        val schema = subplans.head.output
        assert(
          subplans.tail.forall { pl => compareOutputs(pl.output, schema) },
          s"Schemas are different: ${subplans.map { "<"+_.output.map { a => a.name+":"+a.dataType }.mkString(", ")+">" }.mkString("; ") }"
        )
        UnionIterator(subplans.map { RecurTo(_) })
      }
      case Join(left, right, joinType, None, hint) => 
      {
        // outer joins don't make sense without a condition
        assert(joinType == InnerJoin)
        BlockNestedLoopJoinIterator(RecurTo(left), RecurTo(right))
      }
      case Join(left, right, joinType, Some(condition), hint) => 
      {
        val (lhsExprs, rhsExprs, remainder) = 
          JoinPredicates.getEquiJoinAttributes(
            condition, 
            left.outputSet.toSet, 
            right.outputSet.toSet
          )
        assert(lhsExprs.size == rhsExprs.size, "JoinPredicates returned non-equivalent attribute sizes")
        
        var ret: QueryResultIterator = 
          if(lhsExprs.size == 0){
            // no joinable attributes
            BlockNestedLoopJoinIterator(RecurTo(left), RecurTo(right))
          } else {
            HashJoinIterator(
              RecurTo(left), 
              RecurTo(right),
              lhsExprs.map { CompileExpr(_, left) }.toArray, 
              rhsExprs.map { CompileExpr(_, right) }.toArray
            )
          }

        if(remainder.isDefined){
          // use the join itself as a basis, since we're using the joint schema
          val eval = CompileExpr(remainder.get, plan)
          ret = FilterIterator(eval(_).asInstanceOf[Boolean], ret)
        }

        /* return */ ret
      }
      case subplan:Subquery => RecurTo(subplan)
      case View(_, output, subplan) => 
      {
        assert(compareOutputs(output, subplan.output), "View output doesn't match source")
        RecurTo(subplan)
      }
      case _:ReturnAnswer => ???
      case _:Generate => ???
      case _:Intersect => ???
      case _:Except => ???
      case _:InsertIntoDir => ???
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