package org.mimirdb.iskra

import org.specs2.mutable.Specification

import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.functions._
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

  lazy val table_r = 
    spark.read
         .option("header", "true")
         .csv("test_data/r.csv")

  lazy val table_s = 
    spark.read
         .option("header", "true")
         .csv("test_data/s.csv")


  sequential
  "Basic Data Source in Scala" >> {
    compare("Open CSV") {
      table_r
    } 
  }

  "Basic Data Source in SQL" >> {
    compare("Open CSV in SQL") {
      spark.sql("""
        SELECT * FROM csv.`test_data/r.csv`
      """)
    }
  }

  "Second Data Source in SQL" >> {
    compare("Open Second CSV") {
      table_s
    }
  }

  "Filter" >> {
    compare("Filter") {
      table_r.filter { table_r("A") > 2 } 
    }
  }

  "Union" >> {
    compare("Union") {
      table_r.select(
                table_r("A"),
                table_r("B")
             )
             .union(table_s)
    }
  }

  "Nested Loop Join" >> {
    compare("Nested Loop Join") {
      table_r.join(table_s)      
    }
  }

  "Hash-Join" >> {
    compare("Equi Join") {
      table_r.join(table_s, table_r("A") === table_s("A"))      
             .select(table_s("A") as "A",
                     table_r("B") as "B",
                     table_r("C") as "C",
                     table_s("B") as "D")
    }
  }

  "Stacked Hash-Joins" >> {
    compare("Multiple Hash Joins") {
      val df = 
        table_r.join(table_s, table_r("A") === table_s("A"))
               .select(table_s("A") as "A",
                       table_r("B") as "B",
                       table_r("C") as "C",
                       table_s("B") as "D")
      df.join(table_s, df("B") === table_s("B"))
    }
  }
}