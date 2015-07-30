package org.exoplatform.addons.es.index;

/**
 * Created by TClement on 7/29/15.
 */
public interface IndexingService {

  /**
   *
   * Add Indexing Connector to the service
   *
   * @param indexingServiceConnector the indexing connector to add
   */
  void addConnector (IndexingServiceConnector indexingServiceConnector);

  /**
   *
   * Index all document in the indexing queue
   *
   */
  void index();

  /**
   *
   * Add a new document to the index queue
   *
   * @param indexingServiceConnector
   * @param id id of the document
   * @param operation operation to the index {create, update, delete, init}
   */
  void addToIndexQueue(IndexingServiceConnector indexingServiceConnector, Long id, String operation);

}
