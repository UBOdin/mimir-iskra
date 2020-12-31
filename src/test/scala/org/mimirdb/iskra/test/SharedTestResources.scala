package org.mimirdb.iskra.test

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }
import com.typesafe.scalalogging.LazyLogging
import org.mimirdb.iskra.util.Timing

object SharedTestResources 
  extends LazyLogging
  with Timing
{
  lazy val spark = {
    time {
      SparkSession.builder
        .appName("Mimir-Iskra-Test")
        .master("local[*]")
        .getOrCreate()
    }
  }
}

trait SharedTestResources
{
  lazy val spark = SharedTestResources.spark
}
