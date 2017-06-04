package org.pfm.spark.utils

import com.typesafe.config.{ Config, ConfigFactory }
import java.io.File

object Configuration {
  val myConfigFile = new File("config/application.conf")
  val fileConfig = ConfigFactory.parseFile(myConfigFile)
  val props = ConfigFactory.load(fileConfig)
}