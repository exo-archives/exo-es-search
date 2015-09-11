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

import org.apache.commons.lang.StringUtils;
import org.exoplatform.addons.es.client.ElasticIndexingClient;
import org.exoplatform.addons.es.client.ElasticContentRequestBuilder;
import org.exoplatform.addons.es.dao.IndexingOperationDAO;
import org.exoplatform.addons.es.domain.IndexingOperation;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.IndexingServiceConnector;
import org.exoplatform.commons.utils.PropertyManager;
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

  private static final String BATCH_NUMBER_PROPERTY_NAME = "exo.es.indexing.batch.number";
  private static final Integer BATCH_NUMBER_DEFAULT = 1000;

  private static final Log LOG = ExoLogger.getExoLogger(ElasticIndexingService.class);

  public static final String INIT = "I";
  public static final String CREATE = "C";
  public static final String UPDATE = "U";
  public static final String DELETE = "D";
  public static final String DELETE_ALL = "X";

  private Integer batchNumber = BATCH_NUMBER_DEFAULT;

  private final IndexingOperationDAO indexingOperationDAO;

  private final ElasticIndexingClient elasticIndexingClient;

  private final ElasticContentRequestBuilder elasticContentRequestBuilder;

  public ElasticIndexingService(IndexingOperationDAO indexingOperationDAO, ElasticIndexingClient elasticIndexingClient, ElasticContentRequestBuilder elasticContentRequestBuilder) {
    this.indexingOperationDAO = indexingOperationDAO;
    this.elasticIndexingClient = elasticIndexingClient;
    this.elasticContentRequestBuilder = elasticContentRequestBuilder;
    if (StringUtils.isNotBlank(PropertyManager.getProperty(BATCH_NUMBER_PROPERTY_NAME)))
      this.batchNumber = Integer.valueOf(PropertyManager.getProperty(BATCH_NUMBER_PROPERTY_NAME));
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
      addToIndexingQueue(IndexingServiceConnector.getType(), null, INIT);
    }
  }

  @Override
  public void addToIndexingQueue(String connectorName, String id, String operation) {
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
      default: LOG.warn(operation + " is not an accepted operation for the Indexing Queue.");
        break;
    }
  }

  @Override
  public void clearIndexingQueue() {
    indexingOperationDAO.deleteAll();
  }

  /**
   * Handle the Indexing queue
   * Get all data in the indexing queue, transform them to ES requests, send requests to ES
   * This method is call by the job scheduler
   */
  @Override
  public void process() {
    Map<String, Map<String, List<IndexingOperation>>> indexingQueueSorted = new HashMap<>();
    Date startProcessing = new Date(0L);

    List<IndexingOperation> indexingOperations;

    //Loop until the number of data retrieved from indexing queue is less than BATCH_NUMBER (default = 1000)
    do {

      //Get BATCH_NUMBER (default = 1000) first indexing operations
      indexingOperations = indexingOperationDAO.findAllFirst(batchNumber);

      //Get all Indexing operations and order them per operation and type in map: <Operation, <Type, List<IndexingOperation>>>
      for (IndexingOperation indexingOperation : indexingOperations) {
        //Check if the Indexing Operation map already contains a specific operation
        if (!indexingQueueSorted.containsKey(indexingOperation.getOperation())) {
          //If not add a new operation in the map
          indexingQueueSorted.put(indexingOperation.getOperation(), new HashMap<String, List<IndexingOperation>>());
        }
        //Check if the operation map already contains a specific type
        if (!indexingQueueSorted.get(indexingOperation.getOperation()).containsKey(indexingOperation.getEntityType())) {
          //If not add a new type for the operation above
          indexingQueueSorted.get(indexingOperation.getOperation()).put(indexingOperation.getEntityType(), new ArrayList<IndexingOperation>());
        }
        //Add the indexing operation in the specific Operation -> Type
        indexingQueueSorted.get(indexingOperation.getOperation()).get(indexingOperation.getEntityType()).add(indexingOperation);
        //Get the max timestamp of the indexing queue
        if (startProcessing.compareTo(indexingOperation.getTimestamp()) < 0) {
          startProcessing = indexingOperation.getTimestamp();
        }
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
            for (IndexingOperation indexingOperation : indexingQueueSorted.get(DELETE_ALL).get(type)) {
              //Remove the type (= remove all documents of this type) and recreate it
              ElasticIndexingServiceConnector connector = (ElasticIndexingServiceConnector) getConnectors().get(indexingOperation.getEntityType());
              elasticIndexingClient.sendDeleteTypeRequest(connector.getIndex(), connector.getType());
              elasticIndexingClient.sendCreateTypeRequest(connector.getIndex(), connector.getType(), elasticContentRequestBuilder.getCreateTypeRequestContent(connector));
              //Remove all useless CUD operation that was plan before this delete all
              deleteOperationsForTypesBefore(new String[]{CREATE, DELETE}, indexingQueueSorted, indexingOperation);
              deleteOperationsForTypes(new String[]{UPDATE}, indexingQueueSorted, indexingOperation);
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
          for (IndexingOperation deleteIndexQueue : indexingQueueSorted.get(DELETE).get(deleteOperations)) {
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
          for (IndexingOperation createIndexQueue : indexingQueueSorted.get(CREATE).get(createOperations)) {
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
          for (IndexingOperation updateIndexQueue : indexingQueueSorted.get(UPDATE).get(updateOperations)) {
            bulkRequest += elasticContentRequestBuilder.getUpdateDocumentRequestContent((ElasticIndexingServiceConnector) getConnectors().get(updateIndexQueue.getEntityType()), updateIndexQueue.getEntityId());
          }
        }
        //Remove the update operations from the map
        indexingQueueSorted.remove(UPDATE);
      }

      if (!bulkRequest.equals("")) {
        elasticIndexingClient.sendCUDRequest(bulkRequest);
      }

      // Removes the processed IDs from the “indexing queue” table that have timestamp older than the timestamp of
      // start of processing
      indexingOperationDAO.deleteAllBefore(startProcessing);

    } while (indexingOperations.size() >= batchNumber);

    // TODO 4: In case of error, the entityID+entityType will be logged in a “error queue” to allow a manual
    // reprocessing of the indexing operation. However, in a first version of the implementation, the error will only
    // be logged with ERROR level in the log file of platform.

  }

  private void deleteOperationsForTypesBefore(String[] operations, Map<String, Map<String, List<IndexingOperation>>> indexingQueueSorted, IndexingOperation refIindexOperation) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(refIindexOperation.getEntityType())) {
          for (Iterator<IndexingOperation> iterator = indexingQueueSorted.get(operation).get(refIindexOperation.getEntityType()).iterator(); iterator.hasNext();) {
            IndexingOperation indexingOperation = iterator.next();
            //Check timestamp higher than the timestamp of the reference indexing operation, the index operation is removed
            if (refIindexOperation.getTimestamp().compareTo(indexingOperation.getTimestamp()) > 0) {
              iterator.remove();
            }
          }
        }
      }
    }
  }

  private void deleteOperationsForTypes(String[] operations, Map<String, Map<String, List<IndexingOperation>>> indexingQueueSorted, IndexingOperation indexQueue) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(indexQueue.getEntityType())) {
          for (Iterator<IndexingOperation> iterator = indexingQueueSorted.get(operation).get(indexQueue.getEntityType()).iterator(); iterator.hasNext();) {
            iterator.next();
            iterator.remove();
          }
        }
      }
    }
  }

  private void deleteOperationsByEntityIdForTypesBefore(String[] operations, Map<String, Map<String, List<IndexingOperation>>> indexingQueueSorted, IndexingOperation indexQueue) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(indexQueue.getEntityType())) {
          for (Iterator<IndexingOperation> iterator = indexingQueueSorted.get(operation).get(indexQueue.getEntityType()).iterator(); iterator.hasNext();) {
            IndexingOperation indexingOperation = iterator.next();
            //Check timestamp higher than the timestamp of the CUD indexing queue, the index queue is removed
            if (indexQueue.getTimestamp().compareTo(indexingOperation.getTimestamp()) > 0
                && indexingOperation.getEntityId().equals(indexQueue.getEntityId())) {
              iterator.remove();
            }
          }
        }
      }
    }
  }

  private void deleteOperationsByEntityIdForTypes(String[] operations, Map<String, Map<String, List<IndexingOperation>>> indexingQueueSorted, IndexingOperation indexQueue) {
    for (String operation: operations) {
      if (indexingQueueSorted.containsKey(operation)) {
        if (indexingQueueSorted.get(operation).containsKey(indexQueue.getEntityType())) {
          for (Iterator<IndexingOperation> iterator = indexingQueueSorted.get(operation).get(indexQueue.getEntityType()).iterator(); iterator.hasNext();) {
            IndexingOperation indexingOperation = iterator.next();
            if (indexingOperation.getEntityId().equals(indexQueue.getEntityId())) {
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
    IndexingOperation indexingOperation = initIndexingQueue(getConnectors().get(connector), INIT);
    indexingOperationDAO.create(indexingOperation);
  }

  private void addCreateOperation (String connector, String id) {
    IndexingOperation indexingOperation = initIndexingQueue(getConnectors().get(connector), CREATE);
    indexingOperation.setEntityId(id);
    indexingOperationDAO.create(indexingOperation);
  }

  private void addUpdateOperation (String connector, String id) {
    IndexingOperation indexingOperation = initIndexingQueue(getConnectors().get(connector), UPDATE);
    indexingOperation.setEntityId(id);
    indexingOperationDAO.create(indexingOperation);
  }

  private void addDeleteOperation (String connector, String id) {
    IndexingOperation indexingOperation = initIndexingQueue(getConnectors().get(connector), DELETE);
    indexingOperation.setEntityId(id);
    indexingOperationDAO.create(indexingOperation);
  }

  private void addDeleteAllOperation (String connector) {
    IndexingOperation indexingOperation = initIndexingQueue(getConnectors().get(connector), DELETE_ALL);
    indexingOperationDAO.create(indexingOperation);
  }

  private IndexingOperation initIndexingQueue (IndexingServiceConnector IndexingServiceConnector,
                                           String operation) {
    IndexingOperation indexingOperation = new IndexingOperation();
    indexingOperation.setEntityType(IndexingServiceConnector.getType());
    indexingOperation.setOperation(operation);
    return indexingOperation;
  }
}

