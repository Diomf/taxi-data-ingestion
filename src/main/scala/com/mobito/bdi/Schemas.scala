package com.mobito.bdi

import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object Schemas {
  val TRIPS_SCHEMA: StructType = StructType(Seq(
    StructField("date", StringType),
    StructField("vendor_id", StringType),
    StructField("pickup_timestamp", TimestampType),
    StructField("dropoff_timestamp", TimestampType),
    StructField("passenger_count", IntegerType),
    StructField("trip_distance", DoubleType),
    StructField("ratecode_id", StringType),
    StructField("store_and_fwd_flag", StringType),
    StructField("pickup_location_id", StringType),
    StructField("dropoff_location_id", StringType),
    StructField("payment_type", StringType),
    StructField("fare_amount", DecimalType(17, 4)),
    StructField("extra", DecimalType(17, 4)),
    StructField("mta_tax", DecimalType(17, 4)),
    StructField("tip_amount", DecimalType(17, 4)),
    StructField("tolls_amount", DecimalType(17, 4)),
    StructField("improvement_surcharge", DecimalType(17, 4)),
    StructField("total_amount", DecimalType(17, 4))
  ))

  val ZONES_SCHEMA: StructType = StructType(Seq(
    StructField("borough", StringType),
    StructField("location_id", StringType),
    StructField("objectid", StringType),
    StructField("shape_area", DoubleType),
    StructField("shape_leng", DoubleType),
    StructField("zone", StringType)
  ))
}
