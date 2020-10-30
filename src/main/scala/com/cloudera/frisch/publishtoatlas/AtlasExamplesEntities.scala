package com.cloudera.frisch.publishtoatlas

import com.cloudera.frisch.publishtoatlas.config.StandardConfig
import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.atlas.model.notification.HookNotification
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2
import org.apache.hadoop.security.UserGroupInformation
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._


object AtlasExamplesEntities extends AtlasHook with Logging {

  /**
    * A method to create a bunch of examples Spark entities with links and relationships
    * @param i to represent the i_th entity to create
    */
  def createAtlasEntityWithInt(i: Int) : Unit = {

    val entitiesToCreate = new AtlasEntitiesWithExtInfo()

    // Create entities
    val sparkColumnA1Entity = new AtlasEntity()
    sparkColumnA1Entity.setTypeName("spark_column")
    val attributesColumnA1:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesColumnA1.put("qualifiedName", "spark_column-A1-"+i)
    attributesColumnA1.put("name", "Spark Column A1 from Program - " +i)
    attributesColumnA1.put("type", "int")
    sparkColumnA1Entity.setAttributes(attributesColumnA1)
    val sparkColumnA1ObjectId = entityToReference(sparkColumnA1Entity)

    val sparkColumnA2Entity = new AtlasEntity()
    sparkColumnA2Entity.setTypeName("spark_column")
    val attributesColumnA2:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesColumnA2.put("qualifiedName", "spark_column-A2-"+i)
    attributesColumnA2.put("name", "Spark Column A2 from Program - " +i)
    attributesColumnA2.put("type", "string")
    sparkColumnA2Entity.setAttributes(attributesColumnA2)
    val sparkColumnA2ObjectId = entityToReference(sparkColumnA2Entity)

    val sparkTableEntity = new AtlasEntity()
    sparkTableEntity.setTypeName("spark_table")
    val attributesTbl:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesTbl.put("qualifiedName", "spark_table-A-"+i)
    attributesTbl.put("name", "Spark Table A from Program - " +i)
    sparkTableEntity.setAttributes(attributesTbl)
    val sparkTableObjectId = entityToReference(sparkTableEntity)

    val sparkColumnB1Entity = new AtlasEntity()
    sparkColumnB1Entity.setTypeName("spark_column")
    val attributesColumnB1:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesColumnB1.put("qualifiedName", "spark_column-B1-"+i)
    attributesColumnB1.put("name", "Spark Column B1 from Program - " +i)
    attributesColumnB1.put("type", "int")
    sparkColumnB1Entity.setAttributes(attributesColumnB1)
    val sparkColumnB1ObjectId = entityToReference(sparkColumnB1Entity)

    val sparkColumnB2Entity = new AtlasEntity()
    sparkColumnB2Entity.setTypeName("spark_column")
    val attributesColumnB2:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesColumnB2.put("qualifiedName", "spark_column-B2-"+i)
    attributesColumnB2.put("name", "Spark Column B2 from Program - " +i)
    attributesColumnB2.put("type", "int")
    sparkColumnB2Entity.setAttributes(attributesColumnB2)
    val sparkColumnB2ObjectId = entityToReference(sparkColumnB2Entity)

    val sparkTableEntity2 = new AtlasEntity()
    sparkTableEntity2.setTypeName("spark_table")
    val attributesTbl2:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesTbl2.put("qualifiedName", "spark_table-B-"+i)
    attributesTbl2.put("name", "Spark Table B from Program - " +i)
    sparkTableEntity2.setAttributes(attributesTbl2)
    val sparkTable2ObjectId = entityToReference(sparkTableEntity2)

    val sparkProcessEntity = new AtlasEntity()
    sparkProcessEntity.setTypeName("spark_process")
    val attributesProcess:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesProcess.put("qualifiedName", "spark_process-"+i)
    attributesProcess.put("name", "Spark Process from Program - " +i)
    sparkProcessEntity.setAttributes(attributesProcess)
    val sparkProcessObjectId = entityToReference(sparkProcessEntity)

    val sparkAppEntity = new AtlasEntity()
    sparkAppEntity.setTypeName("spark_application")
    val attributes:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributes.put("qualifiedName", "spark_job_fake-"+i)
    attributes.put("name", "Spark Job from Program - " +i)
    sparkAppEntity.setAttributes(attributes)
    val sparkAppObjectId = entityToReference(sparkAppEntity)

    val sparkColumnLineage1Entity = new AtlasEntity()
    sparkColumnLineage1Entity.setTypeName("spark_column_lineage")
    val attributesColumnLineage1:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesColumnLineage1.put("qualifiedName", "spark_column_lineage-1-"+i)
    attributesColumnLineage1.put("name", "Spark Column Lineage 1 from Program - " +i)
    sparkColumnLineage1Entity.setAttributes(attributesColumnLineage1)
    val sparkColumnLineage1ObjectId = entityToReference(sparkColumnLineage1Entity)

    val sparkColumnLineage2Entity = new AtlasEntity()
    sparkColumnLineage2Entity.setTypeName("spark_column_lineage")
    val attributesColumnLineage2:  java.util.Map[String,AnyRef] = new java.util.HashMap[String,AnyRef]()
    attributesColumnLineage2.put("qualifiedName", "spark_column_lineage-2-"+i)
    attributesColumnLineage2.put("name", "Spark Column Lineage 2 from Program - " +i)
    sparkColumnLineage2Entity.setAttributes(attributesColumnLineage2)
    val sparkColumnLineage2ObjectId = entityToReference(sparkColumnLineage2Entity)

    // Create links
    sparkAppEntity.setRelationshipAttribute("processes", Seq(sparkProcessObjectId).asJava)
    sparkProcessEntity.setRelationshipAttribute("application", sparkAppObjectId)

    sparkProcessEntity.setAttribute("inputs", Seq(sparkTableObjectId).asJava)
    sparkProcessEntity.setAttribute("outputs", Seq(sparkTable2ObjectId).asJava)
    sparkTableEntity.setRelationshipAttribute("inputToProcesses", Seq(sparkProcessObjectId).asJava)
    sparkTableEntity2.setRelationshipAttribute("outputFromProcesses", Seq(sparkProcessObjectId).asJava)

    sparkColumnA1Entity.setRelationshipAttribute("table", sparkTableObjectId)
    sparkColumnA2Entity.setRelationshipAttribute("table", sparkTableObjectId)
    sparkTableEntity.setRelationshipAttribute("columns", Seq(sparkColumnA1ObjectId,sparkColumnA2ObjectId).asJava)

    sparkColumnB1Entity.setRelationshipAttribute("table", sparkTable2ObjectId)
    sparkColumnB2Entity.setRelationshipAttribute("table", sparkTable2ObjectId)
    sparkTableEntity.setRelationshipAttribute("columns", Seq(sparkColumnB1ObjectId,sparkColumnB2ObjectId).asJava)

    sparkColumnLineage1Entity.setAttribute("inputs", Seq(sparkColumnA1ObjectId).asJava)
    sparkColumnLineage1Entity.setAttribute("outputs", Seq(sparkColumnB1ObjectId).asJava)
    sparkColumnA1Entity.setRelationshipAttribute("inputToProcesses", Seq(sparkColumnLineage1ObjectId).asJava)
    sparkColumnB1Entity.setRelationshipAttribute("outputFromProcesses", Seq(sparkColumnLineage1ObjectId).asJava)

    sparkColumnLineage2Entity.setAttribute("inputs", Seq(sparkColumnA1ObjectId,sparkColumnA2ObjectId).asJava)
    sparkColumnLineage2Entity.setAttribute("outputs", Seq(sparkColumnB2ObjectId).asJava)
    sparkColumnA1Entity.setRelationshipAttribute("inputToProcesses", Seq(sparkColumnLineage2ObjectId).asJava)
    sparkColumnA2Entity.setRelationshipAttribute("inputToProcesses", Seq(sparkColumnLineage2ObjectId).asJava)
    sparkColumnB2Entity.setRelationshipAttribute("outputFromProcesses", Seq(sparkColumnLineage2ObjectId).asJava)

    // Add entities to main object and create them
    entitiesToCreate.addEntity(sparkColumnA1Entity)
    entitiesToCreate.addEntity(sparkColumnA2Entity)
    entitiesToCreate.addEntity(sparkColumnLineage1Entity)
    entitiesToCreate.addEntity(sparkColumnB1Entity)
    entitiesToCreate.addEntity(sparkColumnB2Entity)
    entitiesToCreate.addEntity(sparkColumnLineage2Entity)
    entitiesToCreate.addEntity(sparkTableEntity2)
    entitiesToCreate.addEntity(sparkProcessEntity)
    entitiesToCreate.addEntity(sparkAppEntity)


    val createRequest = new EntityCreateRequestV2(StandardConfig.kerberosUser, entitiesToCreate): HookNotification
    notifyEntities(Seq(createRequest).asJava, UserGroupInformation.getCurrentUser)

    logger.info("Created entity number : " + i )

  }

  def entityToReference(entity: AtlasEntity): AtlasObjectId = {
    new AtlasObjectId(entity.getTypeName, "qualifiedName", entity.getAttribute("qualifiedName"))
  }



}
