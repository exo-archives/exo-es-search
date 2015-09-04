/* 
* Copyright (C) 2003-2015 eXo Platform SAS.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see http://www.gnu.org/licenses/ .
*/
package org.exoplatform.addons.es.index.elastic;

import org.exoplatform.addons.es.client.ElasticIndexingClient;
import org.exoplatform.addons.es.client.ElasticContentRequestBuilder;
import org.exoplatform.addons.es.dao.IndexingQueueDAO;
import org.exoplatform.addons.es.domain.IndexingQueue;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.IndexingServiceConnector;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.util.*;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/29/15
 */
public class ElasticIndexingService extends IndexingService {

  private static final Integer BATCH_NUMBER = Integer.valueOf((System.getProperty("exo.indexing.batch") != null) ?
      System.getProperty("exo.indexing.batch") : "1000");

  private static final Log LOG = ExoLogger.getExoLogger(ElasticIndexingService.class);

  public static final String INIT = "I";
  public static final String CREATE = "C";
  public static final String UPDATE = "U";
  public static final String DELETE = "D";
  public static final String DELETE_ALL = "X";

  private final IndexingQueueDAO indexingQueueDAO;

  private final ElasticIndexingClient elasticIndexingClient;

  private final ElasticContentRequestBuilder elasticContentRequestBuilder;

  public ElasticIndexingService(IndexingQueueDAO indexingQueueDAO, ElasticIndexingClient elasticIndexingClient, ElasticContentRequestBuilder elasticContentRequestBuilder) {
    this.indexingQueueDAO = indexingQueueDAO;
    this.elasticIndexingClient = elasticIndexingClient;
    this.elasticContentRequestBuilder = elasticContentRequestBuilder;
  }

  @Override
  public void addConnector(IndexingServiceConnector IndexingServiceConnector) {
    if (getConnectors().containsKey(IndexingServiceConnector.getType())) {
      LOG.error("Impossible to add connector " + IndexingServiceConnector.getType()
          + ". A connector with the same name has already been registered.");
    } else {
      getConnectors().put(IndexingServiceConnector.getType(), IndexingServiceConnector);
      LOG.info("A new Indexing Connector has been added: " + IndexingServiceConnector.getType());
      //When a new connector is added, ES create and type need to be created
      addToIndexQueue(IndexingServiceConnector.getType(), null, INIT);
    }
  }

  @Override
  public void addToIndexQueue(String connectorName, String id, String operation) {

    switch (operation) {
      //A new type of document need to be initialise
      case INIT: addInitOperation(connectorName);
        break;
      //A new entity need to be indexed
      case CREATE: addCreateOperation(connectorName, id);
        break;
      //An existing entity need to be updated in the create
      case UPDATE: addUpdateOperation(connectorName, id);
        break;
      //An existing entity need to be deleted from the create
      case DELETE: addDeleteOperation(connectorName, id);
        break;
      //All entities of a specific type need to be deleted
      case DELETE_ALL: addDeleteAllOperation(connectorName);
        break;
      default: LOG.warn(operation + " is not an accepted operation for the Index Queue.");
        break;
    }

  }

  @Override
  public void clearIndexQueue() {
    indexingQueueDAO.deleteAll();
  }

  /**
   *
   * Handle the Indexing queue
   * Get all data in the indexing queue, transform them to ES requests, send requests to ES
   * This method is call by the job scheduler
   *
   */
  @Override
  public void process() {

    Map<String, Map<String, List<IndexingQueue>>> indexingQueueSorted = new HashMap<>();
    Date startProcessing = new Date(0L);

    List<IndexingQueue> indexingQueues;

    //Loop until the number of data retrieved from indexing queue is less than BATCH_NUMBER (default = 1000)
    do {

      //Get BATCH_NUMBER (default = 1000) first indexing queue
      indexingQueues = indexingQueueDAO.findAllFirst(BATCH_NUMBER);

      //Get all Indexing Queue and order them per operation and type in map: <Operation, <Type, List<IndexingQueue>>>
      for (IndexingQueue indexingQueue: indexingQueues) {
        //Check if the Indexing Queue map already contains a specific operation
        if (!indexingQueueSorted.containsKey(indexingQueue.getOperation())) {
          //If not add a new operation in the map
          indexingQueueSorted.put(indexingQueue.getOperation(), new HashMap<String, List<IndexingQueue>>());
        }
        //Check if the operation map already contains a specific type
        if (!indexingQueueSorted.get(indexingQueue.getOperation()).containsKey(indexingQueue.getEntityType())) {
          //If not add a new type for the operation above
          indexingQueueSorted.get(indexingQueue.getOperation()).put(indexingQueue.getEntityType(), new ArrayList<IndexingQueue>());
        }
        //Add the indexing queue in the specific Operation -> Type
        indexingQueueSorted.get(indexingQueue.getOperation()).get(indexingQueue.getEntityType()).add(indexingQueue);
        //Get the max timestamp of the indexing queue
        if (startProcessing.compareTo(indexingQueue.getTimestamp()) < 0) startProcessing = indexingQueue.getTimestamp();
      }

      // Process all the requests for “init of the ES create mapping” (Operation type = I) in the indexing queue (if any)
      if (indexingQueueSorted.containsKey(INIT)) {
        for (String type : indexingQueueSorted.get(INIT).keySet()) {
          sendInitRequests(getConnectors().get(type));
        }
        indexingQueueSorted.remove(INIT);
      }

      // Process all the requests for “remove all documents of type” (Operation type = X) in the indexing queue (if any)
      // = Delete type in ES
      if (indexingQueueSorted.containsKey(DELETE_ALL)) {
        for (String type : indexingQueueSorted.get(DELETE_ALL).keySet()) {
          if (indexingQueueSorted.get(DELETE_ALL).containsKey(type)) {
            for (IndexingQueue indexingQueue: indexingQueueSorted.get(DELETE_ALL).get(type)) {
              //Remove the type (= remove all documents of this type) and recreate it
              ElasticIndexingServiceConnector connector = (ElasticIndexingServiceConnector) getConnectors().get(indexingQueue.getEntityType());
              elasticIndexingClient.sendDeleteTypeRequest(connector.getIndex(), connector.getType());
              elasticIndexingClient.sendCreateTypeRequest(connector.getIndex(), connector.getType(), elasticContentRequestBuilder.getCreateTypeRequestContent(connector));
              //Remove all useless CUD operation that was plan before this delete all
              deleteOperationsForTypesBefore(new String[]{CREATE, DELETE}, indexingQueueSorted, indexingQueue);
              deleteOperationsForTypes(new String[]{UPDATE}, indexingQueueSorted, indexingQueue);
            }
          }
        }
        indexingQueueSorted.remove(DELETE_ALL);
      }

      //Initialise bulk request for CUD operations
      String bulkRequest = "";

      //Process Delete document operation
      if (indexingQueueSorted.containsKey(DELETE)) {
        for (String deleteOperations : indexingQueueSorted.get(DELETE).keySet()) {
          for (IndexingQueue deleteIndexQueue : indexingQueueSorted.get(DELETE).get(deleteOperations)) {
            bulkRequest += elasticContentRequestBuilder.getDeleteDocumentRequestContent((ElasticIndexingServiceConnector) getConnectors().get(deleteIndexQueue.getEntityType()), deleteIndexQueue.getEntityId());
            //Remove the object from other create or update operations planned before the timestamp of the delete operation
            deleteOperationsByEntityIdForTypesBefore(new String[]{CREATE}, indexingQueueSorted, deleteIndexQueue);
            deleteOperationsByEntityIdForTypes(new String[]{UPDATE}, indexingQueueSorted, deleteIndexQueue);
          }
        }
        //Remove the delete operations from the map
        indexingQueueSorted.remove(DELETE);
      }

      //Process Create document operation
      if (indexingQueueSorted.containsKey(CREATE)) {
        for (String createOperations : indexingQueueSorted.get(CREATE).keySet()) {
          for (IndexingQueue createIndexQueue : indexingQueueSorted.get(CREATE).get(createOperations)) {
            bulkRequest += elasticContentRequestBuilder.getCreateDocumentRequestContent((ElasticIndexingServiceConnector) getConnectors().get(createIndexQueue.getEntityType()), createIndexQueue.getEntityId());
            //Remove the object from other update operations for this entityId
            deleteOperationsByEntityIdForTypes(new String[]{UPDATE}, indexingQueueSorted, createIndexQueue);
          }
        }
        //Remove the create operations from the map
        indexingQueueSorted.remove(CREATE);
      }

      //Process Update document operation
      if (indexingQueueSorted.containsKey(UPDATE)) {
        for (String updateOperations : indexingQueueSorted.get(UPDATE).keySet()) {
          for (IndexingQueue updateIndexQueue : indexingQueueSorted.get(UPDATE).get(updateOperations)) {
            bulkRequest += elasticContentRequestBuilder.getUpdateDocumentRequestContent((ElasticIndexingServiceConnector) getConnectors().get(updateIndexQueue.getEntityType()), updateIndexQueue.getEntityId());
          }
        }
        //Remove the update operations from the map
        indexingQueueSorted.remove(UPDATE);
      }

    if (!bulkRequest.equals("")) elasticIndexingClient.sendCUDRequest(bulkRequest);

    // Removes the processed IDs from the “indexing queue” table that have timestamp older than the timestamp of
    // start of processing
    indexingQueueDAO.deleteAllBefore(startProcessing);

  } while (indexingQueues.size() >= BATCH_NUMBER);

  // TODO 4: In case of error, the entityID+entityType will be logged in a “error queue” to allow a manual
  // reprocessing of the indexing operation. However, in a first version of the implementation, the error will only
  // be logged with ERROR level in the log file of platform.

}

  private void deleteOperationsForTypesBefore(String[] operations, Map<String, Map<String, List<IndexingQueue>>> indexingQueueSorted, IndexingQueue indexQueue) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(indexQueue.getEntityType())) {
          for (Iterator<IndexingQueue> iterator = indexingQueueSorted.get(operation).get(indexQueue.getEntityType()).iterator(); iterator.hasNext();) {
            IndexingQueue indexingQueue = iterator.next();
            //Check timestamp higher than the timestamp of the CUD indexing queue, the index queue is removed
            if (indexQueue.getTimestamp().compareTo(indexingQueue.getTimestamp()) > 0) {
              iterator.remove();
            }
          }
        }
      }
    }
  }

  private void deleteOperationsForTypes(String[] operations, Map<String, Map<String, List<IndexingQueue>>> indexingQueueSorted, IndexingQueue indexQueue) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(indexQueue.getEntityType())) {
          for (Iterator<IndexingQueue> iterator = indexingQueueSorted.get(operation).get(indexQueue.getEntityType()).iterator(); iterator.hasNext();) {
            iterator.next();
            iterator.remove();
          }
        }
      }
    }
  }

  private void deleteOperationsByEntityIdForTypesBefore(String[] operations, Map<String, Map<String, List<IndexingQueue>>> indexingQueueSorted, IndexingQueue indexQueue) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(indexQueue.getEntityType())) {
          for (Iterator<IndexingQueue> iterator = indexingQueueSorted.get(operation).get(indexQueue.getEntityType()).iterator(); iterator.hasNext();) {
            IndexingQueue indexingQueue = iterator.next();
            //Check timestamp higher than the timestamp of the CUD indexing queue, the index queue is removed
            if (indexQueue.getTimestamp().compareTo(indexingQueue.getTimestamp()) > 0
                && indexingQueue.getEntityId().equals(indexQueue.getEntityId())) {
              iterator.remove();
            }
          }
        }
      }
    }
  }

  private void deleteOperationsByEntityIdForTypes(String[] operations, Map<String, Map<String, List<IndexingQueue>>> indexingQueueSorted, IndexingQueue indexQueue) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(indexQueue.getEntityType())) {
          for (Iterator<IndexingQueue> iterator = indexingQueueSorted.get(operation).get(indexQueue.getEntityType()).iterator(); iterator.hasNext();) {
            IndexingQueue indexingQueue = iterator.next();
            if (indexingQueue.getEntityId().equals(indexQueue.getEntityId())) {
              iterator.remove();
            }
          }
        }
      }
    }
  }

  private void sendInitRequests(IndexingServiceConnector IndexingServiceConnector) {

    ElasticIndexingServiceConnector connector = (ElasticIndexingServiceConnector) IndexingServiceConnector;

    //Send request to create create
    elasticIndexingClient.sendCreateIndexRequest(
        connector.getIndex(), elasticContentRequestBuilder.getCreateIndexRequestContent(connector));

    //Send request to create type
    elasticIndexingClient.sendCreateTypeRequest(
        connector.getIndex(), connector.getType(), elasticContentRequestBuilder.getCreateTypeRequestContent(connector));

  }

  private void addInitOperation(String connector) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), INIT);
    indexingQueueDAO.create(indexingQueue);
  }

  private void addCreateOperation (String connector, String id) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), CREATE);
    indexingQueue.setEntityId(id);
    indexingQueueDAO.create(indexingQueue);
  }

  private void addUpdateOperation (String connector, String id) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), UPDATE);
    indexingQueue.setEntityId(id);
    indexingQueueDAO.create(indexingQueue);
  }

  private void addDeleteOperation (String connector, String id) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), DELETE);
    indexingQueue.setEntityId(id);
    indexingQueueDAO.create(indexingQueue);
  }

  private void addDeleteAllOperation (String connector) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), DELETE_ALL);
    indexingQueueDAO.create(indexingQueue);
  }

  private IndexingQueue initIndexingQueue (IndexingServiceConnector IndexingServiceConnector,
                                           String operation) {
    IndexingQueue indexingQueue = new IndexingQueue();
    indexingQueue.setEntityType(IndexingServiceConnector.getType());
    indexingQueue.setOperation(operation);
    return indexingQueue;
  }
}

