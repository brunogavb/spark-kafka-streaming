package com.smps.util

import com.google.gson.{JsonObject, JsonParser}
import scala.io.Source

object Configs {

  private def getVaultConfig: JsonObject = {
    val source = Source.fromFile(vaultPath)
    try {
      val text = source.getLines.mkString
      val jsonStringAsObject = new JsonParser().parse(text)
      jsonStringAsObject.getAsJsonObject
    } finally {
      if (source != null) source.close()
    }
  }

  private def defaultReplace(input: String): String = {
    input.replace("-", "_")
  }

  lazy val vaultPath: String = sys.env("VAULT_PATH")
  lazy val productName: String = sys.env("PRODUCT_NAME")
  lazy val projectName: String = sys.env("PROJECT_NAME").replace("-ms", "")
  lazy val collectionName: String = sys.env("MONGODB_COLLECTION_NAME")
  lazy val jsonVaultObject: JsonObject = getVaultConfig

  lazy val mongodbDatabaseName: String = jsonVaultObject.get("databaseName").getAsString
  lazy val checkpointLocation: String = jsonVaultObject.get("checkpointLocation").getAsString
  lazy val dataLocation: String = jsonVaultObject.get("dataLocation").getAsString
  lazy val triggerProcessingTime: String = jsonVaultObject.get("triggerProcessingTime").getAsString
  lazy val eventHubUri: String = s"sb://${sys.env("ENVIRONMENT")}-$productName.servicebus.windows.net"
  lazy val eventHubPort: String = jsonVaultObject.get("eventHubPort").getAsString
  lazy val eventHubTopic: String = s"$projectName-$collectionName"
  lazy val eventHubStartingOffset: String = jsonVaultObject.get("eventHubStartingOffset").getAsString
  lazy val eventHubSessionTimeout: String = jsonVaultObject.get("eventHubSessionTimeout").getAsString
  lazy val eventHubRequestTimeout: String = jsonVaultObject.get("eventHubRequestTimeout").getAsString
  lazy val eventHubKeyName: String = jsonVaultObject.get("eventHubKeyName").getAsString
  lazy val eventHubKeyValue: String = jsonVaultObject.get("eventHubKeyValue").getAsString
  lazy val kafkaBootstrapServers: String = s"$eventHubUri:$eventHubPort"
  lazy val kafkaSasl: String = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=" + s"$eventHubUri/;SharedAccessKeyName=$eventHubKeyName;SharedAccessKey=$eventHubKeyValue" + "\";"
  lazy val adlsClientId: String = jsonVaultObject.get("adlsClientId").getAsString
  lazy val adlsClientSecret: String = jsonVaultObject.get("adlsClientSecret").getAsString
  lazy val adlsTenantId: String = jsonVaultObject.get("adlsTenantId").getAsString
  lazy val adlsAccount: String = jsonVaultObject.get("adlsAccount").getAsString
  lazy val adlsContainer: String = jsonVaultObject.get("adlsContainer").getAsString
  lazy val adlsDefaultFS: String = s"abfss://$adlsContainer@$adlsAccount.dfs.core.windows.net"
  lazy val sparkSecret: String = java.util.UUID.randomUUID.toString
  lazy val relativePath = s"${defaultReplace(productName)}/${defaultReplace(projectName)}/${defaultReplace(collectionName)}"

}