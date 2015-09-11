package org.exoplatform.addons.es.index;

import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by TClement on 7/29/15.
 */
public abstract class IndexingService {

  private static final Log LOG = ExoLogger.getExoLogger(IndexingService.class);

  private static final ThreadLocal<String> CURRENT_TENANT_NAME = new ThreadLocal<>();

  public static void setCurrentTenantName(String tenantName) {
    CURRENT_TENANT_NAME.set(tenantName);
  }
  public static String getCurrentTenantName() {
    return CURRENT_TENANT_NAME.get();
  }

  Map<String, IndexingServiceConnector> connectors = new HashMap<String, IndexingServiceConnector>();

  /**
   *
   * Add Indexing Connector to the service
   *
   * @param indexingServiceConnector the indexing connector to add
   *
   * @LevelAPI Experimental
   */
  public void addConnector (IndexingServiceConnector indexingServiceConnector) {
    if (connectors.containsKey(indexingServiceConnector.getType())) {
      LOG.error("Impossible to add connector " + indexingServiceConnector.getType()
          + ". A connector with the same name has already been registered.");
    } else {
      connectors.put(indexingServiceConnector.getType(), indexingServiceConnector);
      LOG.info("A new Indexing Connector has been added: " + indexingServiceConnector.getType());
    }
  }

  /**
   *
   * Gets all current connectors
   *
   * @return Connectors
   *
   * @LevelAPI Experimental
   */
  public Map<String, IndexingServiceConnector> getConnectors() {
    return connectors;
  }

  /**
   *
   * Add a new document to the create queue
   *
   * @param connectorName Name of the connector
   * @param id id of the document
   * @param operation operation to the create {create, update, delete, init}
   *
   * @LevelAPI Experimental
   */
  public abstract void addToIndexingQueue(String connectorName, String id, String operation);

  /**
   *
   * Index all document in the indexing queue
   *
   * @LevelAPI Experimental
   */
  public abstract void process();

  /**
   *
   * Clear the indexQueue
   *
   * @LevelAPI Experimental
   *
   */
  public abstract void clearIndexingQueue();

}
