package com.mobito.bdi

import com.mobito.bdi.Schemas.TRIPS_SCHEMA
import org.apache.spark.sql.functions.{broadcast, col, date_format, substring, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Parsing {
  private val timestampPattern: String = "dd/MM/yyyy hh:mm:ss a"

  sealed trait LocationType
  case object Pickup extends LocationType
  case object Dropoff extends LocationType

  /** Perform cleaning steps for taxi trips data */
  def cleanTrips(trips: DataFrame): DataFrame = {
    val essentialTripFields = List("tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount", "total_amount")
    val pickup_timestamp = to_timestamp(col("tpep_pickup_datetime"), timestampPattern)
    val dropoff_timestamp = to_timestamp(col("tpep_dropoff_datetime"), timestampPattern)

    /** 1. Remove rows with any essential column being empty
     *  2. Filter out rows with invalid trip_distance (0)
     *  3. Filter out rows with no passengers indicated
     *  4. Filter out rows where pickup_time - dropoff_time is less than a minute */
    trips.na.drop("any", Seq(essentialTripFields: _*))
      .filter(col("passenger_count").cast(IntegerType) > 0)
      .filter(col("trip_distance").cast(DoubleType) =!= 0)
      .filter(col("fare_amount").cast(DecimalType(17, 4)) > 0)
      .filter(
        unix_timestamp(dropoff_timestamp) - unix_timestamp(pickup_timestamp) > 60
      )
  }

  /** Cast trips data to data types, create necessary columns */
  def augmentTrips(trips: DataFrame): DataFrame = {
    trips.select(
      date_format(to_timestamp(col("tpep_pickup_datetime"), timestampPattern), "yyyyMMdd")
        .alias("date"),
      col("VendorID").cast(StringType).alias("vendor_id"),
      to_timestamp(col("tpep_pickup_datetime"), timestampPattern).alias("pickup_timestamp"),
      to_timestamp(col("tpep_dropoff_datetime"), timestampPattern).alias("dropoff_timestamp"),
      col("passenger_count").cast(IntegerType).alias("passenger_count"),
      col("trip_distance").cast(DoubleType).alias("trip_distance"),
      col("RatecodeID").cast(StringType).alias("ratecode_id"),
      col("store_and_fwd_flag").cast(StringType).alias("store_and_fwd_flag"),
      col("PULocationID").cast(StringType).alias("pickup_location_id"),
      col("DOLocationID").cast(StringType).alias("dropoff_location_id"),
      col("payment_type").cast(StringType).alias("payment_type"),
      col("fare_amount").cast(DecimalType(17, 4)).alias("fare_amount"),
      col("extra").cast(DecimalType(17, 4)).alias("extra"),
      col("mta_tax").cast(DecimalType(17, 4)).alias("mta_tax"),
      col("tip_amount").cast(DecimalType(17, 4)).alias("tip_amount"),
      col("tolls_amount").cast(DecimalType(17, 4)).alias("tolls_amount"),
      col("improvement_surcharge").cast(DecimalType(17, 4)).alias("improvement_surcharge"),
      col("total_amount").cast(DecimalType(17, 4)).alias("total_amount")
    )
  }

  /** Enrich trips data with Zone fields based on Pickup location id */
  def enrichBasedOnPickup(trips: DataFrame, zones: DataFrame): DataFrame = {
    val enrichmentCols = List(
      col("borough").alias("pickup_borough"),
      col("zone").alias("pickup_zone"),
      col("shape_area").alias("pickup_shape_area"),
      col("shape_leng").alias("pickup_shape_leng")
    )

    trips.join(zones,
        col("pickup_location_id") === col("location_id"),
        "left")
      .select(TRIPS_SCHEMA.fieldNames.map(col) ++ enrichmentCols: _*)
  }

  /** Enrich trips data with Zone fields based on Pickup/DropOff location id */
  def enrich(trips: DataFrame, zones: DataFrame, basedOn: LocationType): DataFrame = {

    val locationType = basedOn match {
      case Pickup => "pickup"
      case Dropoff => "dropoff"
    }

    val enrichmentCols = List(
      col("borough").alias(s"${locationType}_borough"),
      col("zone").alias(s"${locationType}_zone"),
      col("shape_area").alias(s"${locationType}_shape_area"),
      col("shape_leng").alias(s"${locationType}_shape_leng")
    )

    trips.join(broadcast(zones),
        col(s"${locationType}_location_id") === col("location_id"),
        "left")
      .select(trips.columns.map(col) ++ enrichmentCols: _*)
  }
}
