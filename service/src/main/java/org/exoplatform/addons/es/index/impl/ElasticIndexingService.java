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
package org.exoplatform.addons.es.index.impl;

import org.exoplatform.addons.es.dao.IndexingQueueDAO;
import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.addons.es.domain.IndexingQueue;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.IndexingServiceConnector;
import org.exoplatform.container.PortalContainer;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/29/15
 */
public class ElasticIndexingService extends IndexingService {

  private static final Log LOG = ExoLogger.getExoLogger(ElasticIndexingService.class);

  public static final String INIT = "I";
  public static final String CREATE = "C";
  public static final String UPDATE = "U";
  public static final String DELETE = "D";
  public static final String DELETE_ALL = "X";

  private static final Integer BATCH_NUMBER = Integer.valueOf((System.getProperty("indexing.batch") != null) ?
      System.getProperty("indexing.batch") : "1000");

  private IndexingQueueDAO indexingQueueDAO;

  public ElasticIndexingService() {
    PortalContainer container = PortalContainer.getInstance();
    indexingQueueDAO = container.getComponentInstanceOfType(IndexingQueueDAO.class);
  }

  @Override
  public void addConnector(IndexingServiceConnector indexingServiceConnector) {
    getConnectors().put(indexingServiceConnector.getType(), indexingServiceConnector);
    //When a new connector is added, ES
    addToIndexQueue(indexingServiceConnector, null, INIT);
  }

  @Override
  public void addToIndexQueue(IndexingServiceConnector indexingServiceConnector, Long id, String operation) {

    switch (operation) {
      //A new type of document need to be initialise
      case INIT: addInitOperation(indexingServiceConnector);
        break;
      //A new entity need to be indexed
      case CREATE: addCreateOperation(indexingServiceConnector, id);
        break;
      //An existing entity need to be updated in the index
      case UPDATE: addUpdateOperation(indexingServiceConnector, id);
        break;
      //An existing entity need to be deleted from the index
      case DELETE: addDeleteOperation(indexingServiceConnector, id);
        break;
      //All entities of a specific type need to be deleted
      case DELETE_ALL: addDeleteAllOperation(indexingServiceConnector);
        break;
      default: LOG.info(operation+" is not an accepted operation for the Index Queue.");
        break;
    }

    //TODO : If a row already exists for this entity in the table, it is updated (using “ON DUPLICATE KEY UPDATE”
    // SQL statement)
  }

  private void addInitOperation(IndexingServiceConnector indexingServiceConnector) {
    IndexingQueue indexingQueue = initIndexingQueue(indexingServiceConnector, INIT);
    indexingQueueDAO.create(indexingQueue);
  }

  private void addCreateOperation (IndexingServiceConnector indexingServiceConnector, Long id) {
    IndexingQueue indexingQueue = initIndexingQueue(indexingServiceConnector, CREATE);
    indexingQueue.setEntityId(String.valueOf(id));
    indexingQueueDAO.create(indexingQueue);
  }

  private void addUpdateOperation (IndexingServiceConnector indexingServiceConnector, Long id) {
    IndexingQueue indexingQueue = initIndexingQueue(indexingServiceConnector, UPDATE);
    indexingQueue.setEntityId(String.valueOf(id));
    indexingQueueDAO.create(indexingQueue);
  }

  private void addDeleteOperation (IndexingServiceConnector indexingServiceConnector, Long id) {
    IndexingQueue indexingQueue = initIndexingQueue(indexingServiceConnector, DELETE);
    indexingQueue.setEntityId(String.valueOf(id));
    indexingQueueDAO.create(indexingQueue);
  }

  private void addDeleteAllOperation (IndexingServiceConnector indexingServiceConnector) {
    IndexingQueue indexingQueue = initIndexingQueue(indexingServiceConnector, DELETE_ALL);
    indexingQueueDAO.create(indexingQueue);
  }

  private IndexingQueue initIndexingQueue (IndexingServiceConnector indexingServiceConnector, String operation) {
    IndexingQueue indexingQueue = new IndexingQueue();
    indexingQueue.setEntityType(indexingServiceConnector.getType());
    indexingQueue.setOperation(operation);
    return indexingQueue;
  }

  @Override
  public void index() {

    List<IndexingQueue> indexingQueues;
    Date startIndexing = indexingQueueDAO.getCurrentTimestamp();

    // TODO 1: Process all the requests for “init of the ES index mapping” (Operation type = I) in the indexing queue
    // (if any)
    String initBulkRequest = "";
    indexingQueues = indexingQueueDAO.findQueueBeforeLastTimeByOperation(startIndexing, INIT);
    for (IndexingQueue indexingQueue : indexingQueues) {
      initBulkRequest = createInitRequest(getConnectors().get(indexingQueue.getEntityType()).init());
    }
    sendRequest(initBulkRequest);

    // TODO 2: Process all the requests for “remove all documents of type” (Operation type = X) in the indexing queue
    // (if any)
    String delateAllBulkRequest = "";
    indexingQueues = indexingQueueDAO.findQueueBeforeLastTimeByOperation(startIndexing, DELETE_ALL);
    for (IndexingQueue indexingQueue : indexingQueues) {
      delateAllBulkRequest += createDeleteAllRequest(getConnectors().get(indexingQueue.getEntityType()).deleteAll());
    }
    sendRequest(delateAllBulkRequest);

    // TODO 3: Process the indexing requests (per batchs of 1000 rows max. This value is configurable) In detail:
    // get the timestamp of start of processing
    // get the list of indexing requests from the indexing queue that are older than this timestamp
    // if the operation type is C-reated or U-pdated, obtain the last version of the modified entity from PLF and
    // generate a Json index request
    // if the operation type is D-eleted, generate a Json delete request
    // computes a bulk update request with all the unitary requests
    // submits this request to ES
    // then removes the processed IDs from the “indexing queue” table that have timestamp older than the timestamp of
    // start of processing
    String bulkRequest = "";
    Integer counter = 0;

    List<String> operations = new ArrayList<>();
    operations.add(CREATE);
    operations.add(UPDATE);
    operations.add(DELETE);

    //Get all CREATE, UPDATE and DELETE operations in th indexing queue
    indexingQueues = indexingQueueDAO.findQueueBeforeLastTimeByOperations(startIndexing, operations);

    for (IndexingQueue indexingQueue : indexingQueues) {

      //Increment counter for batch processing
      counter++;

      //Check the operation type
      if (indexingQueue.getOperation().equals(DELETE)) {
        bulkRequest += createDeleteRequest(getConnectors().get(indexingQueue.getEntityType()).delete(indexingQueue.getEntityId()));
      }
      //Create or Update is the same request
      else {
        bulkRequest += createIndexRequests(getConnectors().get(indexingQueue.getEntityType()).index(indexingQueue.getEntityId()));
      }

      //Batch processing
      if (counter > BATCH_NUMBER) {
        sendRequest(bulkRequest);
        //Reinitialisation of the counter and the bulk request
        bulkRequest = "";
        counter = 0;
      }

    }

    // TODO 4: In case of error, the entityID+entityType will be logged in a “error queue” to allow a manual
    // reprocessing of the indexing operation. However, in a first version of the implementation, the error will only
    // be logged with ERROR level in the log file of platform.
  }

  private String createInitRequest(String configJSON) {
    //TODO check the configJSON, provide a default one if null
    return null;
  }

  private String createIndexRequests(Document document) {
    //TODO transform the document to ES index request to insert it in the bulk request
    return null;
  }

  private String createDeleteRequest(String id) {
    //TODO insert document id in a ES deletion request to insert it in the bulk request
    return null;
  }

  private String createDeleteAllRequest(List<String> ids) {
    //TODO insert all document id in ES deletion requests to insert it in the bulk request
    return null;
  }

  private void sendRequest(String request) {
    //TODO send to ES
  }

}

