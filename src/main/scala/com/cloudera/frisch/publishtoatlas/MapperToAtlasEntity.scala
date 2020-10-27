package com.cloudera.frisch.publishtoatlas


import com.cloudera.frisch.publishtoatlas.config.StandardConfig
import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.atlas.model.notification.HookNotification
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2
import org.apache.hadoop.security.UserGroupInformation
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._


object MapperToAtlasEntity extends AtlasHook with Logging {

  /**
    * A function that convert objects passed into Atlas entities and then published it into
    * Kafka Atlas topic defined in properties
    * @param objectsToConvert a List of Objects to convert to Atlas entities
    */
  def createAtlasEntity[T](objectsToConvert: List[T]) : Unit = {

    val entitiesWithExtInfo = new AtlasEntitiesWithExtInfo()

    for(objectToConvert <- objectsToConvert){
      entitiesWithExtInfo.addEntity(convertToAtlasEntity(objectToConvert))
    }

    val createRequest = new EntityCreateRequestV2(StandardConfig.kerberosUser, entitiesWithExtInfo): HookNotification
    notifyEntities(Seq(createRequest).asJava, UserGroupInformation.getCurrentUser)

  }


  def convertToAtlasEntity[T](objectToConvert: T) : AtlasEntity = {
    // TODO: Write Converter to Atlas entity

    val atlasEntityTest = new AtlasEntity()

    // Below is a working example
    atlasEntityTest.setTypeName("spark_application")
    val attributes:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributes.put("qualifiedName", "spark_job_fake")
    attributes.put("name", "Spark Job from Program")
    atlasEntityTest.setAttributes(attributes)


    atlasEntityTest

  }

}
