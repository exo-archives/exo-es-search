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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.io.IOException;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/1/15
 */
public class ElasticIndexingClient {

  private static final String ES_CLIENT_PROPERTY_NAME = "exo.es.client";
  private static final String ES_CLIENT_DEFAULT = "http://127.0.0.1:9200";

  private static final Log LOG = ExoLogger.getExoLogger(ElasticIndexingClient.class);

  private String urlClient = ES_CLIENT_DEFAULT;

  private HttpClient client;

  public ElasticIndexingClient() {
    //Get url client from exo global properties
    if (StringUtils.isNotBlank(PropertyManager.getProperty(ES_CLIENT_PROPERTY_NAME)))
      this.urlClient = PropertyManager.getProperty(ES_CLIENT_PROPERTY_NAME);
    this.client = new DefaultHttpClient();
  }

  //For testing
  public ElasticIndexingClient(String url) {
    this.urlClient = url;
    this.client = new DefaultHttpClient();
  }
  //For testing
  public ElasticIndexingClient(HttpClient client, String url) {
    this.client = client;
    this.urlClient = url;
  }

  /**
   *
   * Send request to ES to create a new index
   *
   */
  public void sendCreateIndexRequest(String index, String settings) {

    sendHttpPostRequest(urlClient + "/" + index + "/", settings);

  }

  /**
   *
   * Send request to ES to create a new type
   *
   */
  public void sendCreateTypeRequest(String index, String type, String mappings) {

    sendHttpPostRequest(urlClient + "/" + index + "/_mapping/" + type, mappings);

  }

  /**
   *
   * Send request to ES to delete a type
   * it's the same that deleting all document from a given type
   *
   *
   */
  public void sendDeleteTypeRequest(String index, String type) {

    sendHttpDeleteRequest(urlClient + "/" + index + "/" + type);

  }

  /**
   *
   * Send request to ES to perform a C-reate, U-pdate or D-elete operation on a ES document
   *
   * @param bulkRequest JSON containing C-reate, U-pdate or D-elete operation
   *
   *
   */
  public void sendCUDRequest (String bulkRequest) {

    sendHttpPostRequest(urlClient + "/_bulk", bulkRequest);

  }

  private void sendHttpPostRequest (String url, String content) {
    try {

      HttpPost httpTypeRequest = new HttpPost(url);
      httpTypeRequest.setEntity(new StringEntity(content, "UTF-8"));
      handleHttpResponse(client.execute(httpTypeRequest));

      LOG.debug("Send request to ES:\n Method = POST \nURI =  " + url + " \nContent = " + content);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void sendHttpDeleteRequest (String url) {

    try {

      HttpDelete httpDeleteRequest = new HttpDelete(url);
      handleHttpResponse(client.execute(httpDeleteRequest));

      LOG.debug("Send request to ES:\n Method = DELETE \nURI =  " + url);

    } catch (IOException e) {
      e.printStackTrace();
    }

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
      //TODO manage error
      LOG.error("Error when trying to send request to ES. The reason is: "
          + IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8"));
    } else {
      LOG.debug("Success request to ES: " + IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8"));
    }
  }


}

