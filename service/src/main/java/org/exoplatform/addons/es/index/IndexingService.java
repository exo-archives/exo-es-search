package org.exoplatform.addons.es.index;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by TClement on 7/29/15.
 */
public abstract class IndexingService {

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
    connectors.put(indexingServiceConnector.getType(), indexingServiceConnector);
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
