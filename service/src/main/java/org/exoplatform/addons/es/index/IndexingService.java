package org.exoplatform.addons.es.index;

import org.exoplatform.addons.es.domain.OperationType;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/22/15
 */
public abstract class IndexingService {
  private static final Log LOG = ExoLogger.getExoLogger(IndexingService.class);
  private static final ThreadLocal<String> CURRENT_TENANT_NAME = new ThreadLocal<>();

  private Map<String, IndexingServiceConnector> connectors = new HashMap<String, IndexingServiceConnector>();

  public static String getCurrentTenantName() {
    return CURRENT_TENANT_NAME.get();
  }

  public static void setCurrentTenantName(String tenantName) {
    CURRENT_TENANT_NAME.set(tenantName);
  }

  /**
   * Add Indexing Connector to the service
   * @param indexingServiceConnector the indexing connector to add
   * @LevelAPI Experimental
   */
  public void addConnector (IndexingServiceConnector indexingServiceConnector) {
    if (connectors.containsKey(indexingServiceConnector.getType())) {
      LOG.error("Impossible to add connector {}. A connector with the same name has already been registered.",
              indexingServiceConnector.getType());
    } else {
      connectors.put(indexingServiceConnector.getType(), indexingServiceConnector);
      LOG.info("A new Indexing Connector has been added: {}", indexingServiceConnector.getType());
    }
  }

  /**
   * Gets all current connectors
   * @return Connectors
   * @LevelAPI Experimental
   */
  public Map<String, IndexingServiceConnector> getConnectors() {
    return connectors;
  }

  /**
   * Add a init operation to the indexing queue to init the index
   * @param connectorName Name of the connector
   * @LevelAPI Experimental
   */
  public void init(String connectorName) {
    addToIndexingQueue(connectorName, null, OperationType.INIT);
  }

  /**
   * Add a create operation to the indexing queue
   * @param connectorName Name of the connector
   * @param id id of the document
   * @LevelAPI Experimental
   */
  public void index(String connectorName, String id) {
    addToIndexingQueue(connectorName, id, OperationType.CREATE);
  }

  /**
   * Add a update operation to the indexing queue
   * @param connectorName Name of the connector
   * @param id id of the document
   * @LevelAPI Experimental
   */
  public void reindex(String connectorName, String id) {
    addToIndexingQueue(connectorName, id, OperationType.UPDATE);
  }

  /**
   * Add a delete operation to the indexing queue
   * @param connectorName Name of the connector
   * @param id id of the document
   * @LevelAPI Experimental
   */
  public void unindex(String connectorName, String id) {
    addToIndexingQueue(connectorName, id, OperationType.DELETE);
  }

  /**
   * Add a delete all type operation to the indexing queue
   * @param connectorName Name of the connector
   * @LevelAPI Experimental
   */
  public void unindexAll(String connectorName) {
    addToIndexingQueue(connectorName, null, OperationType.DELETE_ALL);
  }

  /**
   * Add a new document to the create queue
   * @param connectorName Name of the connector
   * @param id id of the document
   * @param operation operation to the create {create, update, delete, init}
   * @LevelAPI Experimental
   */
  protected abstract void addToIndexingQueue(String connectorName, String id, OperationType operation);

  /**
   * Index all document in the indexing queue
   * @LevelAPI Experimental
   */
  public abstract void process();

  /**
   * Clear the indexQueue
   * @LevelAPI Experimental
   */
  public abstract void clearIndexingQueue();

}
