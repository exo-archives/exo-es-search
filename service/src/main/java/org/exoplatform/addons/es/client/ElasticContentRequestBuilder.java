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

import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingServiceConnector;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/3/15
 */
public class ElasticContentRequestBuilder {

  private static final Log LOG = ExoLogger.getExoLogger(ElasticContentRequestBuilder.class);

  /**
   *
   * Get an ES create Index request content
   *
   * @return JSON containing a create index request content
   *
   */
  public String getCreateIndexRequestContent(ElasticIndexingServiceConnector connector) {

    Map<String, String> indexProperties = new HashMap<>();
    indexProperties.put("number_of_shards", String.valueOf(connector.getShards()));
    indexProperties.put("number_of_replicas", String.valueOf(connector.getReplicas()));
    //TODO do we have to define more ?

    JSONObject indexSettings = new JSONObject();
    for (String setting: indexProperties.keySet()) {
      indexSettings.put(setting, indexProperties.get(setting));
    }
    JSONObject indexJSON = new JSONObject();
    indexJSON.put("settings", indexSettings);

    return indexJSON.toJSONString();
  }

  /**
   *
   * Get an ES create type request content
   * For instance:
   * {
       "type_name" : {
         "properties" : {
          "permissions" : {"type" : "string", "index" : "not_analyzed" }
         }
       }
     }
   *
   * @return JSON containing a create type request content
   *
   */
  public String getCreateTypeRequestContent(ElasticIndexingServiceConnector connector) {

    //If no mapping provided, send the default one
    if (connector.getMapping() == null || connector.getMapping().isEmpty()) {

      JSONObject permissionAttributes = new JSONObject();
      permissionAttributes.put("type", "string");
      permissionAttributes.put("index", "not_analyzed");

      JSONObject permission = new JSONObject();
      permission.put("permissions", permissionAttributes);

      JSONObject mappingProperties = new JSONObject();
      mappingProperties.put("properties",permission);

      JSONObject mappingJSON = new JSONObject();
      mappingJSON.put(connector.getType(), mappingProperties);

      return mappingJSON.toJSONString();
    }
    //Else return the mapping provide by developer
    return connector.getMapping();
  }

  /**
   *
   * Get an ES delete document content to insert in a bulk request
   * For instance:
   * { "delete" : { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" } }
   *
   * @return JSON containing a delete request
   *
   */
  public String getDeleteDocumentRequestContent(ElasticIndexingServiceConnector connector, String id) {

    JSONObject cudHeaderRequest = createCUDHeaderRequestContent(connector, id);

    JSONObject deleteRequest = new JSONObject();
    deleteRequest.put("delete", cudHeaderRequest);

    return deleteRequest.toJSONString()+"\n";
  }

  /**
   *
   * Get an ES create document content to insert in a bulk request
   * For instance:
   * { "create" : { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" } }
   * { "field1" : "value3" }
   *
   * @return JSON containing a create document request
   *
   */
  public String getCreateDocumentRequestContent(ElasticIndexingServiceConnector connector, String id) {

    JSONObject ElasticInformation = createCUDHeaderRequestContent(connector, id);

    Document document = connector.create(id);

    JSONObject createRequest = new JSONObject();
    createRequest.put("create", ElasticInformation);

    String request = createRequest.toJSONString()+"\n"+document.toJSON()+"\n";

    LOG.info("Create request to ES: \n " + request);

    return request;
  }

  /**
   *
   * Get an ES update document content to insert in a bulk request
   * For instance:
   * { "update" : { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" } }
   * { "field1" : "value3" }
   *
   * @return JSON containing an update document request
   *
   */
  public String getUpdateDocumentRequestContent(ElasticIndexingServiceConnector connector, String id) {

    JSONObject ElasticInformation = createCUDHeaderRequestContent(connector, id);

    Document document = connector.update(id);

    JSONObject updateRequest = new JSONObject();
    updateRequest.put("update", ElasticInformation);

    String request = updateRequest.toJSONString()+"\n"+document.toJSON()+"\n";

    LOG.info("Update request to ES: \n " + request);

    return request;
  }

  /**
   *
   * Create an ES content containing information for bulk request
   * For instance:
   * { "_index" : "blog", "_type" : "post", "_id" : "blog_post_1" }
   *
   * @return JSON containing information for bulk request
   *
   */
  private JSONObject createCUDHeaderRequestContent(ElasticIndexingServiceConnector connector, String id) {

    JSONObject CUDHeader = new JSONObject();
    CUDHeader.put("_index", connector.getIndex());
    CUDHeader.put("_type", connector.getType());
    CUDHeader.put("_id", id);

    return CUDHeader;
  }

}

