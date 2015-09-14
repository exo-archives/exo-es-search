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

import org.junit.Test;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/20/15
 */
public class ElasticIndexingIntegrationTest extends AbstractIntegrationTest {

  @Test
  public void testCreateNewIndex() {

    //Given
    assertFalse(indexExists("blog"));

    //When
    elasticIndexingClient.sendCreateIndexRequest("blog", "");

    //Then
    assertTrue(indexExists("blog"));

  }

  @Test
  public void testCreateNewType() {

    //Given
    assertFalse(typeExists("post"));
    elasticIndexingClient.sendCreateIndexRequest("blog", "");

    //When
    elasticIndexingClient.sendCreateTypeRequest("blog", "post", "{\"post\" : {}}");

    //Then
    assertTrue(typeExists("post"));

  }

  @Test
  public void testIndexingDocument() {

    //Given
    assertEquals(0,elasticDocumentNumber());
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
    admin().indices().prepareRefresh().execute().actionGet();

    //Then
    assertEquals(3,elasticDocumentNumber());

  }

  @Test
  public void testDeleteAllIndexedDocument() {

    //Given
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n" +
        "{ \"field1\" : \"value1\" }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
        "{ \"field1\" : \"value2\" }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n" +
        "{ \"field1\" : \"value3\" }\n";
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    admin().indices().prepareRefresh().execute().actionGet();
    assertTrue(typeExists("type1"));
    assertEquals(3, typeDocumentNumber("type1"));

    //When
    elasticIndexingClient.sendDeleteTypeRequest("test", "type1");
    admin().indices().prepareRefresh().execute().actionGet();

    //Then
    //Type is existing but it has no document
    assertTrue(typeExists("type1"));
    assertEquals(0, typeDocumentNumber("type1"));
  }

}

