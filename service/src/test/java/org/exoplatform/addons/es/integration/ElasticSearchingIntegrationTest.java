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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.junit.Before;
import org.junit.Test;

import org.exoplatform.addons.es.client.ElasticSearchingClient;
import org.exoplatform.addons.es.search.ElasticSearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.exoplatform.services.security.ConversationState;
import org.exoplatform.services.security.Identity;
import org.exoplatform.services.security.MembershipEntry;

/**
 * Created by The eXo Platform SAS Author : Thibault Clement
 * tclement@exoplatform.com 9/11/15
 */
public class ElasticSearchingIntegrationTest extends AbstractIntegrationTest {

  ElasticSearchServiceConnector elasticSearchServiceConnector;

  @Before
  public void initServices() {
    Identity identity = new Identity("BCH");
    identity.setMemberships(Arrays.asList(new MembershipEntry("Admin")));
    ConversationState.setCurrent(new ConversationState(identity));

    String url = "http://" + cluster().httpAddresses()[0].getHostName() + ":" + cluster().httpAddresses()[0].getPort();
    ElasticSearchingClient client = new ElasticSearchingClient(url);
    elasticSearchServiceConnector = new ElasticSearchServiceConnector(getInitConnectorParams(), client);
  }

  @Test
  public void testSearchingDocument() {

    // Given
    assertEquals(0, elasticDocumentNumber());

    String mapping = "{ \"properties\" : " + "   {\"permissions\" : "
        + "       {\"type\" : \"string\", \"index\" : \"not_analyzed\"}" + "   }" + "}";
    CreateIndexRequestBuilder cirb = admin().indices().prepareCreate("test").addMapping("type1", mapping);
    cirb.execute().actionGet();
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n"
        + "{ \"field1\" : \"value1\", \"permissions\" : [\"BCH\"] }\n"
        + "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n"
        + "{ \"field1\" : \"value2\", \"permissions\" : [\"BCH\"] }\n"
        + "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n"
        + "{ \"field1\" : \"value3\", \"permissions\" : [\"BCH\"] }\n";
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    // Elasticsearch has near real-time search: document changes are not visible
    // to search immediately,
    // but will become visible within 1 second
    admin().indices().prepareRefresh().execute().actionGet();

    // When
    List<SearchResult> searchResults = new ArrayList<>(elasticSearchServiceConnector.search(null,
                                                                                            "value1",
                                                                                            null,
                                                                                            0,
                                                                                            10,
                                                                                            null,
                                                                                            null));

    // Then
    assertEquals(1, searchResults.size());

  }

  @Test
  public void testExcerpt() throws InterruptedException {

    // Given
    assertEquals(0, elasticDocumentNumber());
    String mapping = "{ \"properties\" : " + "   {\"permissions\" : "
        + "       {\"type\" : \"string\", \"index\" : \"not_analyzed\"}" + "   }" + "}";
    CreateIndexRequestBuilder cirb = admin().indices().prepareCreate("test").addMapping("type1", mapping);
    cirb.execute().actionGet();
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n"
        + "{ \"field1\" : \"value1\", \"permissions\" : [\"BCH\"] }\n"
        + "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n"
        + "{ \"field1\" : \"value2\", \"permissions\" : [\"BCH\"] }\n"
        + "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n"
        + "{ \"field1\" : \"value3\", \"permissions\" : [\"BCH\"] }\n";
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    // Elasticsearch has near real-time search: document changes are not visible
    // to search immediately,
    // but will become visible within 1 second
    admin().indices().prepareRefresh().execute().actionGet();

    // When
    List<SearchResult> searchResults = new ArrayList<>(elasticSearchServiceConnector.search(null,
                                                                                            "value1",
                                                                                            null,
                                                                                            0,
                                                                                            10,
                                                                                            null,
                                                                                            null));

    // Then
    assertEquals("... <strong>value1</strong>", searchResults.get(0).getExcerpt());

  }

  private InitParams getInitConnectorParams() {
    InitParams params = new InitParams();
    PropertiesParam constructorParams = new PropertiesParam();
    constructorParams.setName("constructor.params");
    constructorParams.setProperty("searchType", "test");
    constructorParams.setProperty("displayName", "test");
    constructorParams.setProperty("index", "test");
    constructorParams.setProperty("type", "type1");
    constructorParams.setProperty("searchFields", "field1");
    params.addParam(constructorParams);
    return params;
  }

}
