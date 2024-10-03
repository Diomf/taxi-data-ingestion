package com.mobito.bdi

import com.mobito.bdi.Configuration.ArgumentsParser
import com.mobito.bdi.DataIO.{loadTripsData, loadZonesData, persistToParquet}
import com.mobito.bdi.Parsing.{Dropoff, Pickup, augmentTrips, cleanTrips, enrich, enrichBasedOnPickup}
import com.mobito.bdi.Schemas.{TRIPS_SCHEMA, ZONES_SCHEMA}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, trim}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.storage.StorageLevel


object TaxiDataIngestion {
  private val ETL_NAME: String = "TaxiDataIngestion"
  private val STATUS_SUCCESS = 0
  private val STATUS_FAILURE = 1

  /** Initialize logger */
  val logger: Logger = Logger.getLogger(TaxiDataIngestion.getClass)

  def main(args: Array[String]): Unit = {
    val argsParser = new ArgumentsParser(args)
    val s3Path = argsParser.s3_path()
    val inputDataFile = argsParser.input_data_file()
    val loadAllTripFiles = argsParser.load_all_trips()
    val enrichmentFile = argsParser.enrichment_file()
    val partitionColumn = argsParser.partition_column().split(",")
    val overwrite = argsParser.overwrite()
    val limit =  if (argsParser.limit.isSupplied) argsParser.limit() else 0
    val outputPath = argsParser.output_path()

    val spark = SparkSession.builder()
//      .master("local[*]")
//      .config("spark.hadoop.fs.s3a.access.key", "<>")
//      .config("spark.hadoop.fs.s3a.secret.key", "<>")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .appName(ETL_NAME)
      .getOrCreate()

//    val sedona = SedonaContext.create(spark)

    val start = System.currentTimeMillis()
    val status: Int = try {
      logger.info("DATA INGESTION: Starting TaxiDataIngestion job")

      /** Loading Data */
      val trips: DataFrame = loadTripsData(spark, s3Path, inputDataFile, loadAllTripFiles, limit)
      val zones: DataFrame = loadZonesData(spark, s3Path, enrichmentFile)
      logger.info("DATA INGESTION: Loaded input Trips and Zones data")

      /** Cleaning Steps */
      val tripsCleaned: DataFrame = cleanTrips(trips)
      val tripsAugmented: DataFrame = augmentTrips(tripsCleaned)
      logger.info("DATA INGESTION: Performed cleaning steps for Trips data")

      /** Apply Schemas */
      val tripsWithSchema = spark.createDataFrame(tripsAugmented.rdd, TRIPS_SCHEMA).dropDuplicates()
      val zonesWithSchema = spark.createDataFrame(zones.rdd, ZONES_SCHEMA).dropDuplicates()

      /** Enrichment */
      val enrichedTrips = enrich(tripsWithSchema, zonesWithSchema, Pickup)
      val furtherEnrichedTrips = enrich(enrichedTrips, zonesWithSchema, Dropoff)
      logger.info("DATA INGESTION: Trips data enriched with geospatial columns")

      /** Storing */
      persistToParquet(furtherEnrichedTrips, outputPath, partitionColumn, overwrite)
      logger.info("DATA INGESTION: Output data persisted to parquet")

      val duration = System.currentTimeMillis() - start
      logger.info(s"Total Running time :$duration ms")
      STATUS_SUCCESS
    } catch {
      case e: Exception =>
        logger.error(s"Exception occurred: ${e.getMessage}")
        STATUS_FAILURE
    }
    spark.stop()
    System.exit(status)
  }
}
