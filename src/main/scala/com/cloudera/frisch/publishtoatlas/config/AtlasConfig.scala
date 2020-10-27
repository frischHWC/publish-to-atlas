package com.cloudera.frisch.publishtoatlas.config

object AtlasConfig extends AppConfig {

  val atlasConf = conf.getConfig("atlas")

  val hookTopic = atlasConf.getString("hook-topic")

}
