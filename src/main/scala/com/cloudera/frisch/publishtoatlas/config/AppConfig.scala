package com.cloudera.frisch.publishtoatlas.config

import com.typesafe.config.ConfigFactory

trait AppConfig {

  val conf = ConfigFactory.load()
  val appConf = conf.getConfig("app")

}
