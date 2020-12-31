package org.mimirdb.iskra

import org.specs2.mutable.Specification

import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.mimirdb.iskra.test.SharedTestResources
import com.typesafe.scalalogging.LazyLogging
import org.mimirdb.iskra.util.Timing

class SimpleQueries 
  extends Specification
  with SharedTestResources
  with LazyLogging
  with Timing
{

  def getValues(rows: Seq[Row]): Seq[Seq[Any]] = 
    rows.map { row => (0 until row.schema.size).map { idx => 
      val v = row.get(idx); 
      if(v == null){ v } else { v.toString }
    } }

  def compare(label: String)(df: => DataFrame) = 
  {
    val constructedDF = time(s"$label:Construct") { df }
    val iskraResult = time(s"$label:Iskra") { Compiler(constructedDF) }
    val sparkResult = time(s"$label:Spark") { constructedDF.collect() }
    getValues(iskraResult) must containTheSameElementsAs(getValues(sparkResult))
  }

  sequential
  "Basic Data Source in Scala" >> {
    compare("Open CSV") {
      spark.read
           .option("header", "true")
           .csv("test_data/r.csv")
    } 
  }

  "Basic Data Source in SQL" >> {
    compare("Open CSV in SQL") {
      spark.sql("""
        SELECT * FROM csv.`test_data/r.csv`
      """)
    }
  }
}