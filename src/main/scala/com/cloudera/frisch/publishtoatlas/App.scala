package com.cloudera.frisch.publishtoatlas

import com.cloudera.frisch.publishtoatlas.config.StandardConfig
import org.apache.logging.log4j.scala.Logging


object App extends Logging {

  /**
    * Main function that launches treatment
    * @param args Not needed
    */
  def main(args : Array[String]) {

    logger.info("Starting Program : " + StandardConfig.appName)

    AtlasSetup.setupAtlasConf()

    // TODO: Get objects to push into Atlas as any form
    val objectsToPushInAtlas = List("fakeobject")

    MapperToAtlasEntity.createAtlasEntity(objectsToPushInAtlas)
    MapperToAtlasEntity.deleteAtlasEntity(objectsToPushInAtlas)
    MapperToAtlasEntity.updateAtlasEntity(objectsToPushInAtlas)

    logger.info("Finished Program")
  }

}