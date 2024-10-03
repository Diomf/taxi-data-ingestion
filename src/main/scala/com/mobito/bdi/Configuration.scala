package com.mobito.bdi

import org.rogach.scallop.{ScallopConf, ScallopOption, stringListConverter}

object Configuration {

  class ArgumentsParser(arguments: Seq[String]) extends ScallopConf(arguments) {

    val s3_path: ScallopOption[String] =
      opt[String]("s3_path", required = false, descr = "S3 bucket path",
        default = Some("s3a://mobito-de-take-home-assignment/input_data/"))

    val input_data_file: ScallopOption[String] =
      opt[String]("input_data_file", required = false, descr = "File containing taxi trip data to ingest",
        default = Some("2017_Yellow_Taxi_Trip_Data.csv"))

    val load_all_trips: ScallopOption[Boolean] =
      opt[Boolean]("load_all_trips", required = false, descr = "Load all trip csv files if enabled, default: false",
        default = Some(false))

    val enrichment_file: ScallopOption[String] =
      opt[String]("enrichment_file", required = false, descr = "GeoJson file containing enrichment data",
        default = Some("NYC_Taxi_Zones.geojson"))

    val partition_column: ScallopOption[String] =
      opt[String]("partition_column", required = false, descr = "Partition Column(s), comma-separated",
        default = Some("date"))

    val overwrite: ScallopOption[Boolean] =
      opt[Boolean]("overwrite", required = false, descr = "Overwrite output parquet, default: false",
        default = Some(false))

    val limit: ScallopOption[Int] =
      opt[Int]("limit", required = false, descr = "Limit the trip data to parse if provided")

    val output_path: ScallopOption[String] =
      opt[String]("output_path", required = false, descr = "Full Output path, default candidates sub-dir",
        default = Some("s3a://mobito-de-take-home-assignment/candidates/minas_kalosynakis/"))

    conflicts(input_data_file, List(load_all_trips))
    verify()
  }
}
