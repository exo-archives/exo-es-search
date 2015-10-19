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
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
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

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/1/15
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticIndexingClientTest {

  ElasticIndexingClient elasticIndexingClient;

  @Mock
  HttpClient httpClient;

  @Mock
  HttpResponse httpResponse;

  @Mock
  StatusLine statusLine;

  @Mock
  HttpEntity httpEntity;

  @Captor
  ArgumentCaptor<HttpPost> httpPostRequestCaptor;

  @Captor
  ArgumentCaptor<HttpDelete> httpDeleteRequestCaptor;


  @Before
  public void initMock() throws IOException {
    MockitoAnnotations.initMocks(this);
    elasticIndexingClient = new ElasticIndexingClient("http://localhost:9200", httpClient);
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
    assertEquals("http://localhost:9200/index/", httpPostRequestCaptor.getValue().getURI().toString());
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
    assertEquals("http://localhost:9200/index/_mapping/type", httpPostRequestCaptor.getValue().getURI().toString());
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
    assertEquals("http://localhost:9200/index/type", httpDeleteRequestCaptor.getValue().getURI().toString());
  }

  @Test
  public void sendCUDRequest_IfCUDOperation_bulkRequestShouldBeSentToElastic() throws IOException {

    //Given
    initHttpSuccessRequest();

    //When
    elasticIndexingClient.sendCUDRequest("FakeBulkRequest");

    //Then
    verify(httpClient, times(1)).execute(httpPostRequestCaptor.capture());
    //Check the endpoint
    assertEquals("http://localhost:9200/_bulk", httpPostRequestCaptor.getValue().getURI().toString());
    //Check the content
    assertEquals("FakeBulkRequest", IOUtils.toString(httpPostRequestCaptor.getValue().getEntity().getContent()));

  }


  private void initHttpSuccessRequest() throws IOException {
    when(httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream("Success", "UTF-8") );
  }

  private void initHttpFailedRequest() throws IOException {
    when(httpClient.execute(any(HttpPost.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(400);
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream("Failed", "UTF-8") );
  }

}

