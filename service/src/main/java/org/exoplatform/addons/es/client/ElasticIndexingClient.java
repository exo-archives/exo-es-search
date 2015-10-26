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
import org.apache.http.client.HttpClient;
import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/1/15
 */
public class ElasticIndexingClient extends ElasticClient {

  private static final Log LOG = ExoLogger.getExoLogger(ElasticIndexingClient.class);

  private static final String ES_INDEX_CLIENT_PROPERTY_NAME = "exo.es.index.server.url";
  private static final String ES_INDEX_CLIENT_PROPERTY_USERNAME = "exo.es.index.server.username";
  private static final String ES_INDEX_CLIENT_PROPERTY_PASSWORD = "exo.es.index.server.password";

  public ElasticIndexingClient() {
    super();
    //Get url client from exo global properties
    if (StringUtils.isNotBlank(PropertyManager.getProperty(ES_INDEX_CLIENT_PROPERTY_NAME))) {
      this.urlClient = PropertyManager.getProperty(ES_INDEX_CLIENT_PROPERTY_NAME);
    }
  }

  public ElasticIndexingClient(String urlClient, HttpClient client) {
    super(urlClient, client);
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
   * Send request to ES to delete a type
   * it's the same that deleting all document from a given type
   */
  public void sendDeleteTypeRequest(String index, String type) {
    sendHttpDeleteRequest(urlClient + "/" + index + "/" + type);
  }

  /**
   * Send request to ES to perform a C-reate, U-pdate or D-elete operation on a ES document
   * @param bulkRequest JSON containing C-reate, U-pdate or D-elete operation
   */
  public void sendCUDRequest (String bulkRequest) {
    sendHttpPostRequest(urlClient + "/_bulk", bulkRequest);
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

