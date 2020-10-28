package com.cloudera.frisch.publishtoatlas.config

object KafkaConfig extends AppConfig {

  val kafkaConf = conf.getConfig("kafka")
  val kafkaConfSecurity = kafkaConf.getConfig("security")
  val kafkaConfKerberos = kafkaConfSecurity.getConfig("kerberos")
  val kafkaConfTls = kafkaConfSecurity.getConfig("tls")

  val brokers = kafkaConf.getString("brokers")
  val acks = kafkaConf.getString("acks")
  val protocol = kafkaConfSecurity.getString("protocol")

  val mechanism = kafkaConfKerberos.getString("mechanism")
  val serviceName = kafkaConfKerberos.getString("service-name")
  val loginModule = kafkaConfKerberos.getString("login-module")
  val loginModuleName = kafkaConfKerberos.getString("login-module-name")
  val storeKey = kafkaConfKerberos.getString("store-key")
  val useKeytab = kafkaConfKerberos.getString("use-keytab")
  val useTicketCache = kafkaConfKerberos.getString("use-ticket-cache")

  val keystoreLocation = kafkaConfTls.getString("keystore.location")
  val keystorePassword = kafkaConfTls.getString("keystore.password")
  val keystoreKeyPassword = kafkaConfTls.getString("keystore.key-password")

  val truststoreLocation = kafkaConfTls.getString("truststore.location")
  val truststorePassword = kafkaConfTls.getString("truststore.password")

}
