package skiers.controller;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;
import skiers.Constants;

/**
 * Component to check DynamoDB connection at application startup
 */
@Component
public class DynamoDBHealthCheck implements ApplicationListener<ApplicationReadyEvent> {
  private static final Logger logger = LoggerFactory.getLogger(DynamoDBHealthCheck.class);

  private final AmazonDynamoDB amazonDynamoDB;

  @Autowired
  public DynamoDBHealthCheck(AmazonDynamoDB amazonDynamoDB) {
    this.amazonDynamoDB = amazonDynamoDB;
  }

  @Override
  public void onApplicationEvent(ApplicationReadyEvent event) {
    try {
      logger.info("Testing DynamoDB connection...");

      // Test connection by listing tables
      ListTablesResult listTablesResult = amazonDynamoDB.listTables(new ListTablesRequest().withLimit(10));
      List<String> tableNames = listTablesResult.getTableNames();

      logger.info("Successfully connected to DynamoDB!");
      logger.info("Found {} tables:", tableNames.size());

      // Check if our target table exists
      boolean tableFound = tableNames.contains(Constants.TARGET_TABLE_NAME);
      if (tableFound) {
        logger.info("Target table '{}' exists.", Constants.TARGET_TABLE_NAME);
      } else {
        logger.warn("⚠️ Target table '{}' does not exist! The application may fail.", Constants.TARGET_TABLE_NAME);
      }

    } catch (Exception e) {
      logger.error("❌ Failed to connect to DynamoDB: {}", e.getMessage(), e);
      logger.error("The application may not function correctly without DynamoDB access.");
    }
  }

  /**
   * Utility method to check connection that can be called manually
   * @return true if connection is successful, false otherwise
   */
  public boolean checkConnection() {
    try {
      amazonDynamoDB.listTables(new ListTablesRequest().withLimit(1));
      return true;
    } catch (Exception e) {
      logger.error("Failed to connect to DynamoDB: {}", e.getMessage());
      return false;
    }
  }
}