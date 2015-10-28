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
package org.exoplatform.addons.es.client;

import org.apache.commons.lang.StringUtils;

import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.util.Map;
import java.util.Set;

/**
 * Created by The eXo Platform SAS Author : Thibault Clement
 * tclement@exoplatform.com 9/1/15
 */
public class ElasticIndexingClient extends ElasticClient {
  private static final Log    LOG                               = ExoLogger.getExoLogger(ElasticIndexingClient.class);
  private static final String ES_INDEX_CLIENT_PROPERTY_NAME     = "exo.es.index.server.url";
  private static final String ES_INDEX_CLIENT_PROPERTY_USERNAME = "exo.es.index.server.username";
  private static final String ES_INDEX_CLIENT_PROPERTY_PASSWORD = "exo.es.index.server.password";
  private ElasticIndexingAuditTrail auditTrail;

  public ElasticIndexingClient(ElasticIndexingAuditTrail auditTrail) {
    super();
    if (auditTrail==null) {
      throw new IllegalArgumentException("AuditTrail is null");
    }
    this.auditTrail = auditTrail;
    // Get url client from exo global properties
    if (StringUtils.isNotBlank(PropertyManager.getProperty(ES_INDEX_CLIENT_PROPERTY_NAME))) {
      this.urlClient = PropertyManager.getProperty(ES_INDEX_CLIENT_PROPERTY_NAME);
      LOG.info("Using {} as Indexing URL", this.urlClient);
    } else {
      LOG.info("Using default as Indexing URL");
    }
  }

  /**
   * Send request to ES to create a new index
   */
  public void sendCreateIndexRequest(String index, String settings) {
    sendHttpPostRequest(urlClient + "/" + index + "/", settings);
  }

  /**
   * Send request to ES to create a new type
   */
  public void sendCreateTypeRequest(String index, String type, String mappings) {
    sendHttpPostRequest(urlClient + "/" + index + "/_mapping/" + type, mappings);
  }

  /**
   * Send request to ES to delete a type it's the same that deleting all
   * document from a given type
   */
  public void sendDeleteTypeRequest(String index, String type) {
    sendHttpDeleteRequest(urlClient + "/" + index + "/" + type);
  }

  /**
   * Send request to ES to perform a C-reate, U-pdate or D-elete operation on a
   * ES document
   * 
   * @param bulkRequest JSON containing C-reate, U-pdate or D-elete operation
   */
  public void sendCUDRequest(String bulkRequest) {
    long startTime = System.currentTimeMillis();
    String response = sendHttpPostRequest(urlClient + "/_bulk", bulkRequest);
    logBulkResponse(response, (System.currentTimeMillis()-startTime));
  }

  private void logBulkResponse(String response, long executionTime) {
    try {
      Object parsedResponse = JSONValue.parseWithException(response);
      if (!(parsedResponse instanceof JSONObject)) {
        LOG.error("Unable to parse Bulk response: response is not a JSON. response={}", response);
        throw new ElasticClientException("Unable to parse Bulk response: response is not a JSON.");
      }
      //process items
      Object items = ((JSONObject) parsedResponse).get("items");
      if (items != null) {
        if (!(items instanceof JSONArray)) {
          LOG.error("Unable to parse Bulk response: items is not a JSONArray. items={}", items);
          throw new ElasticClientException("Unable to parse Bulk response: items is not a JSONArray.");
        }
        for (Object item : ((JSONArray) items).toArray()) {
          if (!(item instanceof JSONObject)) {
            LOG.error("Unable to parse Bulk response: item is not a JSONObject. item={}", item);
            throw new ElasticClientException("Unable to parse Bulk response: item is not a JSONObject.");
          }
          logBulkResponseItem((JSONObject) item, executionTime);
        }
      }
    } catch (ParseException e) {
      throw new ElasticClientException("Unable to parse Bulk response", e);
    }
  }

  private void logBulkResponseItem(JSONObject item, long executionTime) {
    for(Map.Entry operation : (Set<Map.Entry>)item.entrySet()) {
      String operationName = (String) operation.getKey();
      JSONObject operationDetails = (JSONObject) operation.getValue();
      String index = (String) operationDetails.get("_index");
      String type = (String) operationDetails.get("_type");
      String id = (String) operationDetails.get("_id");
      Long status = (Long) operationDetails.get("status");
      String error = (String) operationDetails.get("error");
      if ((status>=200) && (status<=299)) {
        auditTrail.audit(operationName, id, index, type, status.intValue(), error, executionTime);
      } else {
        auditTrail.logRejectedDocument(operationName, id, index, type, status.intValue(), error, executionTime);
      }
    }
  }

  @Override
  protected String getEsUsernameProperty() {
    return PropertyManager.getProperty(ES_INDEX_CLIENT_PROPERTY_USERNAME);
  }

  @Override
  protected String getEsPasswordProperty() {
    return PropertyManager.getProperty(ES_INDEX_CLIENT_PROPERTY_PASSWORD);
  }

}
