package com.cloudera.frisch.publishtoatlas.config

object ZookeeperConfig extends AppConfig {

  val zkConf = conf.getConfig("zookeeper")

  val connect = zkConf.getString("connect")

}
