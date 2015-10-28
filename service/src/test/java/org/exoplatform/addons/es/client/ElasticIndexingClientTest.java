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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import org.exoplatform.commons.utils.PropertyManager;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/1/15
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticIndexingClientTest {
  private ElasticIndexingClient elasticIndexingClient;
  @Mock
  private HttpClient httpClient;
  @Mock
  private HttpResponse httpResponse;
  @Mock
  private StatusLine statusLine;
  @Mock
  private ElasticIndexingAuditTrail auditTrail;
  @Mock
  private HttpEntity httpEntity;
  @Captor
  private ArgumentCaptor<HttpPost> httpPostRequestCaptor;
  @Captor
  private ArgumentCaptor<HttpDelete> httpDeleteRequestCaptor;


  @Before
  public void initMock() throws IOException {
    MockitoAnnotations.initMocks(this);
    PropertyManager.setProperty("exo.es.index.server.url", "http://127.0.0.1:9200");
    elasticIndexingClient = new ElasticIndexingClient(auditTrail);
    elasticIndexingClient.client = httpClient;
  }

  @Test
  public void sendCreateIndexRequest_IfCreateNewIndex_requestShouldBeSentToElastic() throws IOException {

    //Given
    initHttpSuccessRequest();

    //When
    elasticIndexingClient.sendCreateIndexRequest("index", "fakeSettings");

    //Then
    verify(httpClient, times(1)).execute(httpPostRequestCaptor.capture());
    //Check the endpoint
    assertEquals("http://127.0.0.1:9200/index/", httpPostRequestCaptor.getValue().getURI().toString());
    //Check the content
    assertEquals("fakeSettings", IOUtils.toString(httpPostRequestCaptor.getValue().getEntity().getContent()));

  }

  @Test
  public void sendCreateTypeRequest_IfCreateNewType_requestShouldBeSentToElastic() throws IOException {

    //Given
    initHttpSuccessRequest();

    //When
    elasticIndexingClient.sendCreateTypeRequest("index", "type", "fakeMappings");


    //Then
    verify(httpClient, times(1)).execute(httpPostRequestCaptor.capture());
    //Check the endpoint
    assertEquals("http://127.0.0.1:9200/index/_mapping/type", httpPostRequestCaptor.getValue().getURI().toString());
    //Check the content
    assertEquals("fakeMappings", IOUtils.toString(httpPostRequestCaptor.getValue().getEntity().getContent()));

  }

  @Test
  public void sendDeleteTypeRequest_IfCreateNewType_deleteRequestShouldBeSentToElastic() throws IOException {

    //Given
    initHttpSuccessRequest();

    //When
    elasticIndexingClient.sendDeleteTypeRequest("index", "type");

    //Then
    verify(httpClient, times(1)).execute(httpDeleteRequestCaptor.capture());
    //Check the endpoint
    assertEquals("http://127.0.0.1:9200/index/type", httpDeleteRequestCaptor.getValue().getURI().toString());
  }

  @Test
  public void sendCUDRequest_IfCUDOperation_bulkRequestShouldBeSentToElastic() throws IOException {

    //Given
    initHttpSuccessRequest();
    String response = "{\"took\":15," +
        "\"errors\":true," +
        "\"items\":[" +
        "{\"index\":{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\"1\",\"_version\":3,\"status\":200}}" +
        "]}";
    when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(response, "UTF-8"));

    //When
    elasticIndexingClient.sendCUDRequest("FakeBulkRequest");

    //Then
    verify(httpClient, times(1)).execute(httpPostRequestCaptor.capture());
    //Check the endpoint
    assertEquals("http://127.0.0.1:9200/_bulk", httpPostRequestCaptor.getValue().getURI().toString());
    //Check the content
    assertEquals("FakeBulkRequest", IOUtils.toString(httpPostRequestCaptor.getValue().getEntity().getContent()));

  }

  private void initHttpSuccessRequest() throws IOException {
    when(httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream("Success", "UTF-8"));
  }

  @Test
  public void sendBulkRequest_onError_logErrors() throws IOException {
    //Given
    String response = "{\"took\":15," +
        "\"errors\":true," +
        "\"items\":[" +
        "{\"index\":{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\"1\",\"_version\":3,\"status\":200}}," +
        "{\"delete\":{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\"2\",\"_version\":1,\"status\":404,\"found\":false}}," +
        "{\"create\":{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\"3\",\"status\":409,\"error\":\"DocumentAlreadyExistsException[[test][4] [type1][3]: document already exists]\"}}," +
        "{\"update\":{\"_index\":\"index1\",\"_type\":\"type1\",\"_id\":\"1\",\"status\":404,\"error\":\"DocumentMissingException[[index1][-1] [type1][1]: document missing]\"}}" +
        "]}";
    initHttpSuccessRequest();
    when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(response, "UTF-8"));
    //When
    elasticIndexingClient.sendCUDRequest("myBulk");
    //Then
    verify(auditTrail).audit(eq("index"), eq("1"), eq("test"), eq("type1"), eq(HttpStatus.SC_OK), isNull(String.class), anyLong());
    verify(auditTrail).logRejectedDocument(eq("delete"), eq("2"), eq("test"), eq("type1"), eq(HttpStatus.SC_NOT_FOUND), isNull(String.class), anyLong());
    verify(auditTrail).logRejectedDocument(eq("create"), eq("3"), eq("test"), eq("type1"), eq(HttpStatus.SC_CONFLICT), eq("DocumentAlreadyExistsException[[test][4] [type1][3]: document already exists]"), anyLong());
    verify(auditTrail).logRejectedDocument(eq("update"), eq("1"), eq("index1"), eq("type1"), eq(HttpStatus.SC_NOT_FOUND), eq("DocumentMissingException[[index1][-1] [type1][1]: document missing]"), anyLong());
    verifyNoMoreInteractions(auditTrail);
  }

  @Test
  public void sendReindexAll_whateverTheResult_audit() throws IOException {
    //TODO
  }

  @Test
  public void sendDeleteAll_whateverTheResult_audit() throws IOException {
    //TODO
  }

  @Test
  public void sendBulkRequest_ifLoggerLevelError_noCallToAudit() throws IOException {
    //TODO
  }
}

