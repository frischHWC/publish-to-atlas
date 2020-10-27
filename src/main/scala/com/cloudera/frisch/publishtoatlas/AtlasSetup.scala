package com.cloudera.frisch.publishtoatlas

import com.cloudera.frisch.publishtoatlas.config.{AtlasConfig, KafkaConfig, StandardConfig, ZookeeperConfig}
import org.apache.atlas.ApplicationProperties
import org.apache.logging.log4j.scala.Logging

object AtlasSetup extends Logging {

  /**
    * Goal of this method is to setup Atlas configuration using existing configuration passed through application.conf
    * Note that file atlas-application.properties in resources is required to be able to load first configuration (even if it is empty)
    */
  def setupAtlasConf(): Unit = {

    val atlasConf = ApplicationProperties.get()

    atlasConf.setProperty("atlas.kafka.zookeeper.session.timeout.ms", ZookeeperConfig.sessionTimeout)
    atlasConf.setProperty("atlas.kafka.zookeeper.connection.timeout.ms", ZookeeperConfig.connectionTimeout)
    atlasConf.setProperty("atlas.kafka.zookeeper.sync.time.ms", ZookeeperConfig.syncTime)
    atlasConf.setProperty("atlas.kafka.zookeeper.connect", ZookeeperConfig.connect)

    atlasConf.setProperty("atlas.kafka.bootstrap.servers", KafkaConfig.brokers )
    atlasConf.setProperty("atlas.kafka.acks", KafkaConfig.acks )
    atlasConf.setProperty("atlas.kafka.security.protocol", KafkaConfig.protocol)
    atlasConf.setProperty("atlas.authentication.method.kerberos", KafkaConfig.activated)
    atlasConf.setProperty("atlas.kafka.sasl.kerberos.service.name", KafkaConfig.serviceName)
    atlasConf.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", KafkaConfig.loginModule)
    atlasConf.setProperty("atlas.jaas.KafkaClient.loginModuleName", KafkaConfig.loginModuleName)
    atlasConf.setProperty("atlas.jaas.KafkaClient.option.serviceName", KafkaConfig.serviceName)

    atlasConf.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag", KafkaConfig.loginModule)
    atlasConf.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleName", KafkaConfig.loginModuleName)
    atlasConf.setProperty("atlas.jaas.ticketBased-KafkaClient.option.useTicketCache", KafkaConfig.useTicketCache)

    atlasConf.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", KafkaConfig.useKeytab)
    atlasConf.setProperty("atlas.jaas.KafkaClient.option.storeKey", KafkaConfig.storeKey)
    atlasConf.setProperty("atlas.jaas.KafkaClient.option.useTicketCache", KafkaConfig.useTicketCache)
    atlasConf.setProperty("atlas.jaas.KafkaClient.option.keyTab", StandardConfig.keytab)
    atlasConf.setProperty("atlas.jaas.KafkaClient.option.principal", StandardConfig.kerberosUser)

    atlasConf.setProperty("atlas.notification.hook.topic.name", AtlasConfig.hookTopic)


  }

}
