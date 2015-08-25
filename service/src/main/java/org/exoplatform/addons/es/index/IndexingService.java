package org.exoplatform.addons.es.index;

import org.exoplatform.addons.es.index.elastic.ElasticIndexingServiceConnector;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by TClement on 7/29/15.
 */
public abstract class IndexingService {

  private static final Log LOG = ExoLogger.getExoLogger(IndexingService.class);

  Map<String, ElasticIndexingServiceConnector> connectors = new HashMap<String, ElasticIndexingServiceConnector>();

  /**
   *
   * Add Indexing Connector to the service
   *
   * @param elasticIndexingServiceConnector the indexing connector to add
   *
   * @LevelAPI Experimental
   */
  public void addConnector (ElasticIndexingServiceConnector elasticIndexingServiceConnector) {
    if (connectors.containsKey(elasticIndexingServiceConnector.getType())) {
      LOG.error("Impossible to add connector " + elasticIndexingServiceConnector.getType()
          + ". A connector with the same name has already been registered.");
    } else {
      connectors.put(elasticIndexingServiceConnector.getType(), elasticIndexingServiceConnector);
      LOG.error("A new Indexing Connector has been added: " + elasticIndexingServiceConnector.getType());
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
  public Map<String, ElasticIndexingServiceConnector> getConnectors() {
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
  public abstract void addToIndexQueue(String connectorName, Long id, String operation);

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
  public abstract void clearIndexQueue();

}
