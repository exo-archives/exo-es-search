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
package org.exoplatform.addons.es.integration;

/*

WARNING:
THIS TEST CAN WORK ONLY WHEN YOU START A LOCAL ES ON YOUR MACHINE FIRST.
IT"S  ALL TESTS ARE COMMENTED. IF YOU WANT TO TEST IT START LOCAL ES AND UNCOMMENT THE TEST.
TO FIX IT WE NEED TO BE ABLE TO START AN EMBEDDED ES

 */

import static org.junit.Assert.*;

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/20/15
 */
public class ElasticIndexingIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testCreateNewIndex() throws ExecutionException, InterruptedException {
    //Given
    assertFalse(node.client().admin().indices().prepareExists("blog").execute().actionGet().isExists());
    //When
    elasticIndexingClient.sendCreateIndexRequest("blog", "");
    //Then
    assertTrue(node.client().admin().indices().prepareExists("blog").execute().actionGet().isExists());

  }

  @Test
  public void testCreateNewType() throws ExecutionException, InterruptedException {
    //Given
    assertFalse(typeExists("blog", "post"));
    elasticIndexingClient.sendCreateIndexRequest("blog", "");
    //When
    elasticIndexingClient.sendCreateTypeRequest("blog", "post", "{\"post\" : {}}");
    //Then
    assertTrue(typeExists("blog", "post"));

  }

  @Test
  public void testIndexingDocument() {
    //Given
    assertEquals(0, documentNumber());
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n" +
        "{ \"field1\" : \"value1\" }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
        "{ \"field1\" : \"value2\" }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n" +
        "{ \"field1\" : \"value3\" }\n";

    //When
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    //Elasticsearch has near real-time search: document changes are not visible to search immediately,
    // but will become visible within 1 second
    node.client().admin().indices().prepareRefresh().execute().actionGet();

    //Then
    assertEquals(3, documentNumber());

  }

  @Test
  @Ignore
  public void testDeleteType() {
    System.out.println(">>>>>>>>>>>>>> testDeleteType");
    //Given
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n" +
        "{ \"field1\" : \"value1\" }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
        "{ \"field1\" : \"value2\" }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n" +
        "{ \"field1\" : \"value3\" }\n";
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    node.client().admin().indices().prepareRefresh().execute().actionGet();
    assertTrue(typeExists("test", "type1"));
    assertEquals(3, documentNumber("type1"));

    //When
    elasticIndexingClient.sendDeleteTypeRequest("test", "type1");
    node.client().admin().indices().prepareRefresh().execute().actionGet();

    //Then
    assertFalse(typeExists("test", "type1"));
  }

  private boolean typeExists(String index, String type) {
    return node.client().admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists();
  }

  private long documentNumber() {
    return node.client().prepareCount().execute().actionGet().getCount();
  }

  private long documentNumber(String type) {
    return node.client().prepareCount("_all").setQuery(QueryBuilders.termQuery("_type", type)).execute().actionGet().getCount();
  }
}

