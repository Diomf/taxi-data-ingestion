package com.mobito

import org.json4s.BuildInfo

package object bdi {
  val TAXI_DATA_INGESTION_VERSION: String = BuildInfo.version
  val TAXI_DATA_INGESTION_JAR: String = {
    val url = BuildInfo.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    url.substring(url.lastIndexOf('/') + 1, url.length())
  }
}
