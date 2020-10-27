package com.cloudera.frisch.publishtoatlas.config

object ZookeeperConfig extends AppConfig {

  val zkConf = conf.getConfig("zookeeper")

  val connect = zkConf.getString("connect")
  val syncTime = zkConf.getString("sync-time")
  val sessionTimeout = zkConf.getString("session-timeout")
  val connectionTimeout = zkConf.getString("connection-timeout")

}
