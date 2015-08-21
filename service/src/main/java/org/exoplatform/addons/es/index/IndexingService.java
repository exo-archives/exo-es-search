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
      LOG.error("A new Indexing Connector has been added: " + indexingServiceConnector.getType());
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
   * Add a new document to the index queue
   *
   * @param connectorName Name of the connector
   * @param id id of the document
   * @param operation operation to the index {create, update, delete, init}
   *
   * @LevelAPI Experimental
   */
  public abstract void addToIndexQueue(String connectorName, Long id, String operation);

  /**
   *
   * Index all document in the indexing queue
   *
   * @LevelAPI Experimental
   */
  public abstract void index();

  /**
   *
   * Clear the indexQueue
   *
   * @LevelAPI Experimental
   *
   */
  public abstract void clearIndexQueue();

}
