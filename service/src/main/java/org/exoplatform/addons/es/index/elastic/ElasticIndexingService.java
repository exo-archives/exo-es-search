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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.addons.es.dao.IndexingQueueDAO;
import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.addons.es.domain.IndexingQueue;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.container.PortalContainer;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

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

  public static final String ES_CLIENT = (System.getProperty("exo.elasticsearch.client") != null) ?
      System.getProperty("exo.elasticsearch.client") : "http://127.0.0.1:9200";
  private static final Integer BATCH_NUMBER = Integer.valueOf((System.getProperty("exo.indexing.batch") != null) ?
      System.getProperty("exo.indexing.batch") : "1000");

  private String urlClient;

  private IndexingQueueDAO indexingQueueDAO;

  public ElasticIndexingService() {
    PortalContainer container = PortalContainer.getInstance();
    indexingQueueDAO = container.getComponentInstanceOfType(IndexingQueueDAO.class);
    this.urlClient = ES_CLIENT;
  }

  //For Test
  public ElasticIndexingService(String urlClient) {
    PortalContainer container = PortalContainer.getInstance();
    indexingQueueDAO = container.getComponentInstanceOfType(IndexingQueueDAO.class);
    this.urlClient = urlClient;
  }

  @Override
  public void addConnector(ElasticIndexingServiceConnector elasticIndexingServiceConnector) {
    super.addConnector(elasticIndexingServiceConnector);
    //When a new connector is added, ES create and type need to be created
    addToIndexQueue(elasticIndexingServiceConnector.getType(), null, INIT);
  }

  @Override
  public void addToIndexQueue(String connectorName, Long id, String operation) {

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
      default: LOG.info(operation+" is not an accepted operation for the Index Queue.");
        break;
    }

    //TODO : If a row already exists for this entity in the table, it is updated (using “ON DUPLICATE KEY UPDATE”
    // SQL statement)
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

    List<IndexingQueue> indexingQueues;
    Date startProcessing = indexingQueueDAO.getCurrentTimestamp();

    // Process all the requests for “init of the ES create mapping” (Operation type = I) in the indexing queue (if any)
    indexingQueues = indexingQueueDAO.findQueueBeforeLastTimeByOperation(startProcessing, INIT);
    for (IndexingQueue indexingQueue : indexingQueues) {
      sendInitRequests(getConnectors().get(indexingQueue.getEntityType()));
    }


    // Process all the requests for “remove all documents of type” (Operation type = X) in the indexing queue (if any)
    // = Delete type in ES
    indexingQueues = indexingQueueDAO.findQueueBeforeLastTimeByOperation(startProcessing, DELETE_ALL);
    for (IndexingQueue indexingQueue : indexingQueues) {
      sendDeleteTypeRequest(getConnectors().get(indexingQueue.getEntityType()));
    }

    //Process the indexing requests (Operation type = C or U or D)

    List<String> operations = new ArrayList<>();
    operations.add(CREATE);
    operations.add(UPDATE);
    operations.add(DELETE);

    //Get all CREATE, UPDATE and DELETE operations in th indexing queue
    indexingQueues = indexingQueueDAO.findQueueBeforeLastTimeByOperations(startProcessing, operations);

    if (indexingQueues.size() > 0) {

      String bulkRequest = "";
      Integer counter = 0;

      for (IndexingQueue indexingQueue : indexingQueues) {

        //Increment counter for batch processing
        counter++;

        //Check the operation type
        if (indexingQueue.getOperation().equals(DELETE)) {
          bulkRequest += getDeleteRequest(indexingQueue);
        } else if (indexingQueue.getOperation().equals(CREATE)) {
          bulkRequest += getCreateRequest(indexingQueue);
        } else if (indexingQueue.getOperation().equals(UPDATE)) {
          bulkRequest += getUpdateRequest(indexingQueue);
        } else {
          LOG.error(indexingQueue.getOperation() + " is not a valid operation for indexing queue.");
        }

        //Batch processing
        if (counter > BATCH_NUMBER) {
          sendCUDRequest(bulkRequest);
          //Reinitialisation of the counter and the bulk request
          bulkRequest = "";
          counter = 0;
        }

      }

      sendCUDRequest(bulkRequest);
    }

    // TODO 4: In case of error, the entityID+entityType will be logged in a “error queue” to allow a manual
    // reprocessing of the indexing operation. However, in a first version of the implementation, the error will only
    // be logged with ERROR level in the log file of platform.

    // Removes the processed IDs from the “indexing queue” table that have timestamp older than the timestamp of
    // start of processing
    indexingQueueDAO.DeleteAllBefore(startProcessing);

  }

  private void sendInitRequests(ElasticIndexingServiceConnector elasticIndexingServiceConnector) {

    //Send request to create create
    sendCreateIndexRequest(elasticIndexingServiceConnector);

    //Send request to create type
    sendCreateTypeRequest(elasticIndexingServiceConnector);

  }

  /**
   *
   * Send request to ES to create a new create
   *
   * @param elasticIndexingServiceConnector
   *
   */
  private void sendCreateIndexRequest(ElasticIndexingServiceConnector elasticIndexingServiceConnector) {
    try {

      HttpClient client = new DefaultHttpClient();

      //TODO check if index already exist (can been already created by another type of the application)

      HttpPost httpIndexRequest = new HttpPost(urlClient + "/" + elasticIndexingServiceConnector.getIndex() + "/");
      httpIndexRequest.setEntity(
          new StringEntity(getIndexJSON(getIndexingSettings(elasticIndexingServiceConnector)), "UTF-8")
      );
      handleHttpResponse(client.execute(httpIndexRequest));

      LOG.info("New Index created on ES: "+ elasticIndexingServiceConnector.getIndex());

    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   *
   * Send request to ES to create a new type
   *
   * @param elasticIndexingServiceConnector
   *
   */
  private void sendCreateTypeRequest(ElasticIndexingServiceConnector elasticIndexingServiceConnector) {
    try {

      HttpClient client = new DefaultHttpClient();

      HttpPost httpTypeRequest = new HttpPost(urlClient + "/" + elasticIndexingServiceConnector.getIndex()
          + "/_mapping/" + elasticIndexingServiceConnector.getType() );
      httpTypeRequest.setEntity(new StringEntity(elasticIndexingServiceConnector.init(), "UTF-8"));
      handleHttpResponse(client.execute(httpTypeRequest));

      LOG.info("New Type created on ES: " + elasticIndexingServiceConnector.getType() + " for Index: "
          + elasticIndexingServiceConnector.getIndex());

    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   *
   * Send request to ES to delete a type
   * it's the same that deleting all document from a given type
   *
   * @param elasticIndexingServiceConnector
   *
   *
   */
  private void sendDeleteTypeRequest(ElasticIndexingServiceConnector elasticIndexingServiceConnector) {
    try {

      HttpClient client = new DefaultHttpClient();

      HttpDelete httpDeleteRequest = new HttpDelete(urlClient + "/" + elasticIndexingServiceConnector.getIndex() + "/"
          + elasticIndexingServiceConnector.getType() );
      handleHttpResponse(client.execute(httpDeleteRequest));

      LOG.info("Delete request send to ES, type: " + elasticIndexingServiceConnector.getType());

    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   *
   * Send request to ES to perform a C-reate, U-pdate or D-elete operation on a ES document
   *
   * @param bulkRequest JSON containing C-reate, U-pdate or D-elete operation
   *
   *
   */
  private void sendCUDRequest (String bulkRequest) {

    try {

      HttpClient client = new DefaultHttpClient();

      HttpPost httpCUDRequest = new HttpPost(urlClient + "/_bulk");
      httpCUDRequest.setEntity(new StringEntity(bulkRequest, "UTF-8"));
      handleHttpResponse(client.execute(httpCUDRequest));

      LOG.info("CUD Bulk Request send to ES: \n" + bulkRequest);
      System.out.println("CUD Bulk Request send to ES: \n" + bulkRequest);

    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   *
   * Get indexing setting from an indexing service connector
   *
   * @param elasticIndexingServiceConnector
   *
   * @return JSON containing a delete request
   *
   */
  private Map<String, String> getIndexingSettings(ElasticIndexingServiceConnector elasticIndexingServiceConnector) {
    Map<String, String> indexSettings = new HashMap<>();
    indexSettings.put("number_of_shards", String.valueOf(elasticIndexingServiceConnector.getShards()));
    indexSettings.put("number_of_replicas", String.valueOf(elasticIndexingServiceConnector.getReplicas()));
    return indexSettings;
  }

  /**
   *
   * Get an ES delete request to insert in a bulk request
   * For instance:
   * { "delete" : { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" } }
   *
   * @param indexingQueue the indexing queue containing the delete information
   *
   * @return JSON containing a delete request
   *
   */
  private String getDeleteRequest(IndexingQueue indexingQueue) {

    JSONObject cudHeaderRequest = createCUDHeaderRequest(indexingQueue);

    JSONObject deleteRequest = new JSONObject();
    deleteRequest.put("delete", cudHeaderRequest);

    return deleteRequest.toJSONString()+"\n";
  }

  /**
   *
   * Get an ES create document request to insert in a bulk request
   * For instance:
   * { "create" : { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" } }
   * { "field1" : "value3" }
   *
   * @param indexingQueue the indexing queue containing the create information
   *
   * @return JSON containing a create document request
   *
   */
  private String getCreateRequest(IndexingQueue indexingQueue) {

    JSONObject ElasticInformation = createCUDHeaderRequest(indexingQueue);

    Document document = getConnectors().get(indexingQueue.getEntityType()).create(indexingQueue.getEntityId());

    JSONObject createRequest = new JSONObject();
    createRequest.put("create", ElasticInformation);

    String request = createRequest.toJSONString()+"\n"+document.toJSON()+"\n";

    LOG.info("Create request to ES: \n " + request);

    return request;
  }

  /**
   *
   * Get an ES update request to insert in a bulk request
   * For instance:
   * { "update" : { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" } }
   * { "field1" : "value3" }
   *
   * @param indexingQueue the indexing queue containing the update information
   *
   * @return JSON containing an update document request
   *
   */
  private String getUpdateRequest(IndexingQueue indexingQueue) {

    JSONObject ElasticInformation = createCUDHeaderRequest(indexingQueue);

    Document document = getConnectors().get(indexingQueue.getEntityType()).create(indexingQueue.getEntityId());

    JSONObject createRequest = new JSONObject();
    createRequest.put("update", ElasticInformation);

    String request = createRequest.toJSONString()+"\n"+document.toJSON()+"\n";

    LOG.info("Update request to ES: \n " + request);

    return request;
  }

  /**
   *
   * Create an ES request containing information for bulk request
   * For instance:
   * { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" }
   *
   * @param indexingQueue the indexing queue containing the create information
   *
   * @return JSON containing information for bulk request
   *
   */
  private JSONObject createCUDHeaderRequest(IndexingQueue indexingQueue) {

    ElasticIndexingServiceConnector elasticIndexingServiceConnector =
        getConnectors().get(indexingQueue.getEntityType());

    JSONObject CUDHeader = new JSONObject();
    CUDHeader.put("_index", elasticIndexingServiceConnector.getIndex());
    CUDHeader.put("_type", elasticIndexingServiceConnector.getType());
    CUDHeader.put("_id", indexingQueue.getId());

    return CUDHeader;
  }

  /**
   *
   * Handle Http response receive from ES
   * Log an INFO if the return status code is 200
   * Log an ERROR if the return code is different from 200
   *
   * @param httpResponse The Http Response to handle
   *
   */
  private void handleHttpResponse(HttpResponse httpResponse) throws IOException {
    if (httpResponse.getStatusLine().getStatusCode() != 200) {
      //LOG.error("Error when trying to send request to ES. The reason is: "
      //    + IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8"));
      System.out.println(("Error when trying to send request to ES. The reason is: "
          + IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8")));
    } else {
      //LOG.info("Success request to ES: " + IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8"));
      System.out.println(("Success request to ES: "
          + IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8")));
    }
  }

  /**
   *
   * Transform the indexing setting to JSON format
   * For instance:
   * {
   "settings" :
   {
   "number_of_shards" : "5",
   "number_of_replicas" : "1",
   "other_setting" : "value of my setting"
   }
   }
   *
   * @param settings to transform to JSON
   *
   * @return Index settings in JSON format
   *
   */
  private String getIndexJSON(Map<String, String> settings) {

    JSONObject indexSettings = new JSONObject();
    for (String setting: settings.keySet()) {
      indexSettings.put(setting, settings.get(setting));
    }
    JSONObject indexJSON = new JSONObject();
    indexJSON.put("settings", indexSettings);

    return indexJSON.toJSONString();
  }


  private void addInitOperation(String connector) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), INIT);
    indexingQueueDAO.create(indexingQueue);
  }

  private void addCreateOperation (String connector, Long id) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), CREATE);
    indexingQueue.setEntityId(String.valueOf(id));
    indexingQueueDAO.create(indexingQueue);
  }

  private void addUpdateOperation (String connector, Long id) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), UPDATE);
    indexingQueue.setEntityId(String.valueOf(id));
    indexingQueueDAO.create(indexingQueue);
  }

  private void addDeleteOperation (String connector, Long id) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), DELETE);
    indexingQueue.setEntityId(String.valueOf(id));
    indexingQueueDAO.create(indexingQueue);
  }

  private void addDeleteAllOperation (String connector) {
    IndexingQueue indexingQueue = initIndexingQueue(getConnectors().get(connector), DELETE_ALL);
    indexingQueueDAO.create(indexingQueue);
  }

  private IndexingQueue initIndexingQueue (ElasticIndexingServiceConnector elasticIndexingServiceConnector,
                                           String operation) {
    IndexingQueue indexingQueue = new IndexingQueue();
    indexingQueue.setEntityType(elasticIndexingServiceConnector.getType());
    indexingQueue.setOperation(operation);
    return indexingQueue;
  }


}

