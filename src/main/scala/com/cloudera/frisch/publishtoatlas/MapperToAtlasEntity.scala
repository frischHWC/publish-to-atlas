package com.cloudera.frisch.publishtoatlas



import com.cloudera.frisch.publishtoatlas.config.StandardConfig
import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.atlas.model.notification.HookNotification
import org.apache.atlas.model.notification.HookNotification.{EntityCreateRequestV2, EntityDeleteRequestV2, EntityUpdateRequestV2}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._


object MapperToAtlasEntity extends AtlasHook with Logging {

  /**
    * A function that convert objects passed into Atlas entities and then published it into
    * Kafka Atlas topic defined in properties to create these entities
    * @param objectsToConvert a List of Objects to convert to Atlas entities
    */
  def createAtlasEntity[T](objectsToConvert: List[T]) : Unit = {

    val entitiesToCreate = new AtlasEntitiesWithExtInfo()

    for(objectToConvert <- objectsToConvert){
      entitiesToCreate.addEntity(convertToAtlasCreateEntity(objectToConvert))
    }

    val createRequest = new EntityCreateRequestV2(StandardConfig.kerberosUser, entitiesToCreate): HookNotification

    notifyEntities(Seq(createRequest).asJava, UserGroupInformation.getCurrentUser)

  }


  def convertToAtlasCreateEntity[T](objectToConvert: T) : AtlasEntity = {
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

  /**
    * A function that convert objects passed into Atlas objects IDs and then published it into
    * Kafka Atlas topic defined in properties in order to delete these entities
    * @param objectsToConvert a List of Objects to convert to Atlas entities
    */
  def deleteAtlasEntity[T](objectsToConvert: List[T]) : Unit = {

    val entitiesToDelete = new java.util.ArrayList[AtlasObjectId]()

    for(objectToConvert <- objectsToConvert){
      entitiesToDelete.add(convertToAtlasDeleteEntity(objectToConvert))
    }

    val deleteRequest = new EntityDeleteRequestV2(StandardConfig.kerberosUser, entitiesToDelete): HookNotification

    notifyEntities(Seq(deleteRequest).asJava, UserGroupInformation.getCurrentUser)

  }


  def convertToAtlasDeleteEntity[T](objectToConvert: T) : AtlasObjectId = {
    // TODO: Write Converter to Atlas object ID

    val atlasEntityTest = new AtlasObjectId()

    // Below is a working example
    atlasEntityTest.setTypeName("spark_application")
    val attributes:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributes.put("qualifiedName", "spark_job_fake")
    attributes.put("name", "Spark Job from Program")
    atlasEntityTest.setUniqueAttributes(attributes)

    atlasEntityTest

  }

  /**
    * A function that convert objects passed into Atlas objects IDs and then published it into
    * Kafka Atlas topic defined in properties in order to update these entities
    * @param objectsToConvert a List of Objects to convert to Atlas entities
    */
  def updateAtlasEntity[T](objectsToConvert: List[T]) : Unit = {

    val entitiesToCreate = new AtlasEntitiesWithExtInfo()

    for(objectToConvert <- objectsToConvert){
      entitiesToCreate.addEntity(convertToAtlasUpdateEntity(objectToConvert))
    }

    val deleteRequest = new EntityUpdateRequestV2(StandardConfig.kerberosUser, entitiesToCreate): HookNotification

    notifyEntities(Seq(deleteRequest).asJava, UserGroupInformation.getCurrentUser)

  }


  def convertToAtlasUpdateEntity[T](objectToConvert: T) : AtlasEntity = {
    // TODO: Write Converter to Atlas entity

    val atlasEntityTest = new AtlasEntity()

    // Below is a working example
    atlasEntityTest.setTypeName("spark_application")
    val attributes:  java.util.Map[String, AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributes.put("qualifiedName", "spark_job_fake")
    attributes.put("name", "Spark Job from Program - Updated")
    atlasEntityTest.setAttributes(attributes)

    atlasEntityTest

  }

}
