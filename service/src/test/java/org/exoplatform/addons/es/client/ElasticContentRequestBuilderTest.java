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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/4/15
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticContentRequestBuilderTest {

  ElasticContentRequestBuilder elasticContentRequestBuilder;

  @Mock
  ElasticIndexingServiceConnector elasticIndexingServiceConnector;

  @Mock
  Document document;

  @Captor
  ArgumentCaptor<String> stringArgumentCaptor;

  @Before
  public void initMocks() throws ParseException {
    MockitoAnnotations.initMocks(this);
    elasticContentRequestBuilder = new ElasticContentRequestBuilder();
    initElasticServiceConnectorMock();
  }


  @Test
  public void getCreateIndexRequestContent_ifSimpleConnector_shouldReturnIndexJSONSettings() {

    //Todo transform to Json to verify result

    //assertEquals("{\"settings\":{\"number_of_replicas\":\"2\",\"number_of_shards\":\"3\"}}",
        //elasticContentRequestBuilder.getCreateIndexRequestContent(elasticIndexingServiceConnector));

  }

  @Test
  public void getCreateTypeRequestContent_ifSimpleConnectorWithNoMapping_shouldReturnDefaultTypeJSONMapping() {

    //Todo transform to Json to verify result

    //assertEquals("{\"type1\":{\"properties\":{\"permissions\":{\"index\":\"not_analyzed\",\"type\":\"string\"}}}}",
        //elasticContentRequestBuilder.getCreateTypeRequestContent(elasticIndexingServiceConnector));

  }

  @Test
  public void getDeleteDocumentRequestContent_ifSimpleConnectorAndEntityId_shouldReturnDeleteQuery() {

    //Todo transform to Json to verify result

    //assertEquals("{\"delete\":{\"_type\":\"type1\",\"_id\":\"1\",\"_index\":\"test\"}}\n",
        //elasticContentRequestBuilder.getDeleteDocumentRequestContent(elasticIndexingServiceConnector, "1"));

  }

  @Test
  public void getCreateDocumentRequestContent_ifSimpleConnectorAndEntityId_shouldReturnCreateQuery() throws ParseException {

    //Todo transform to Json to verify result

    //Given
    initDocumentMock();

    //Then
    /*assertEquals("{\"create\":{\"_type\":\"type1\",\"_id\":\"1\",\"_index\":\"test\"}}\n" +
            "{\"author\":\"Michael Jordan\",\"quote\":\"I've missed more than 9000 shots in my career. " +
            "I've lost almost 300 games. 26 times, I've been trusted to take the game winning shot and missed. " +
            "I've failed over and over and over again in my life. And that is why I succeed.\"" +
            ",\"permissions\":[\"vizir\",\"goleador\"],\"createdDate\":601146000000,\"url\":\"MyUrlBaby\"}\n",
        elasticContentRequestBuilder.getCreateDocumentRequestContent(elasticIndexingServiceConnector, "1"));
        */
  }

  @Test
  public void getUpdateDocumentRequestContent_ifSimpleConnectorAndEntityId_shouldReturnUpdateQuery() throws ParseException {

    //Todo transform to Json to verify result

    //Given
    initDocumentMock();

    //Then
    /*assertEquals("{\"update\":{\"_type\":\"type1\",\"_id\":\"1\",\"_index\":\"test\"}}\n" +
            "{\"author\":\"Michael Jordan\",\"quote\":\"I've missed more than 9000 shots in my career. " +
            "I've lost almost 300 games. 26 times, I've been trusted to take the game winning shot and missed. " +
            "I've failed over and over and over again in my life. And that is why I succeed.\"" +
            ",\"permissions\":[\"vizir\",\"goleador\"],\"createdDate\":601146000000,\"url\":\"MyUrlBaby\"}\n",
        elasticContentRequestBuilder.getUpdateDocumentRequestContent(elasticIndexingServiceConnector, "1"));
        */

  }

  private void initElasticServiceConnectorMock() {
    when(elasticIndexingServiceConnector.getIndex()).thenReturn("test");
    when(elasticIndexingServiceConnector.getType()).thenReturn("type1");
    when(elasticIndexingServiceConnector.getReplicas()).thenReturn(2);
    when(elasticIndexingServiceConnector.getShards()).thenReturn(3);
    when(elasticIndexingServiceConnector.create("1")).thenReturn(document);
    when(elasticIndexingServiceConnector.update("1")).thenReturn(document);
  }

  private void initDocumentMock() throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    when(document.getCreatedDate()).thenReturn(sdf.parse("19/01/1989"));
    when(document.getId()).thenReturn("1");
    when(document.getUrl()).thenReturn("MyUrlBaby");
    when(document.getPermissions()).thenReturn(new String[]{"vizir","goleador"});
    Map<String, String> fields = new HashMap<>();
    fields.put("quote", "I've missed more than 9000 shots in my career. I've lost almost 300 games. " +
        "26 times, I've been trusted to take the game winning shot and missed. I've failed over and over " +
        "and over again in my life. And that is why I succeed.");
    fields.put("author", "Michael Jordan");
    when(document.getFields()).thenReturn(fields);
    when(document.toJSON()).thenCallRealMethod();
  }

}

