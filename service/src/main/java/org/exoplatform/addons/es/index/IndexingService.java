package org.exoplatform.addons.es.index;

/**
 * Created by TClement on 10/12/15.
 */
public interface IndexingService {

  /**
   * Add a init operation to the indexing queue to init the index
   * @param connectorName Name of the connector
   * @LevelAPI Experimental
   */
  public void init(String connectorName);

  /**
   * Add a create operation to the indexing queue
   * @param connectorName Name of the connector
   * @param id id of the document
   * @LevelAPI Experimental
   */
  public void index(String connectorName, String id);

  /**
   * Add a update operation to the indexing queue
   * @param connectorName Name of the connector
   * @param id id of the document
   * @LevelAPI Experimental
   */
  public void reindex(String connectorName, String id);

  /**
   * Add a delete operation to the indexing queue
   * @param connectorName Name of the connector
   * @param id id of the document
   * @LevelAPI Experimental
   */
  public void unindex(String connectorName, String id);

  /**
   * Add a reindex all operation to the indexing queue
   * @param connectorName Name of the connector
   * @LevelAPI Experimental
   */
  public void reindexAll(String connectorName);

  /**
   * Add a delete all type operation to the indexing queue
   * @param connectorName Name of the connector
   * @LevelAPI Experimental
   */
  public void unindexAll(String connectorName);

}
