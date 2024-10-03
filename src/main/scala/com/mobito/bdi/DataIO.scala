package com.mobito.bdi

import com.mobito.bdi.Schemas.{TRIPS_SCHEMA, ZONES_SCHEMA}
import com.mobito.bdi.TaxiDataIngestion.logger
import org.apache.spark.sql.functions.{col, explode, trim}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataIO {

  /** Repartition based on partition column to redistribute the data and persist output Trip data to parquet */
  def persistToParquet(trips: DataFrame, outputPath: String, partitionColumn: Array[String], overwrite: Boolean):
  Unit = {
    val t0 = System.currentTimeMillis()

    val dirName = "trips_by_" + partitionColumn.mkString("_")
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    trips.repartition(partitionColumn.map(col): _*)
      .write
      .mode(mode)
      .partitionBy(partitionColumn: _*)
      .parquet(outputPath + dirName)

    val t1 = System.currentTimeMillis()
    val duration = t1 - t0
    logger.info(s"Total time to store data :$duration ms")
  }

  /** Load specific taxi trips file or all of them */
  def loadTripsData(spark: SparkSession, s3Path: String, inputDataFile: String, loadAllTrips: Boolean, limit: Int):
  DataFrame = {
    val paths = if (loadAllTrips) "*.csv" else inputDataFile
    val trips: DataFrame = spark
      .read
      .option("header", true)
      .csv(s3Path + paths)

    if (limit > 0)
      trips.limit(limit)
    else trips
  }

  /** Load specific taxi zones file */
  def loadZonesData(spark: SparkSession, s3Path: String, enrichmentFile: String): DataFrame = {
    val zones = spark
      .read
      .option("multiLine", "true")
      .json(s3Path + enrichmentFile)
      .select(explode(col("features")).alias("feature"))
      .select(col("feature.properties.*"))
      .select(
        trim(col("borough")).cast(StringType).alias("borough"),
        trim(col("location_id")).cast(StringType).alias("location_id"),
        trim(col("objectid")).cast(StringType).alias("objectid"),
        trim(col("shape_area")).cast(DoubleType).alias("shape_area"),
        trim(col("shape_leng")).cast(DoubleType).alias("shape_leng"),
        trim(col("zone")).cast(StringType).alias("zone")
      )
    zones
  }
}
