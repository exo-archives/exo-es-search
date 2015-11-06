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

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestController;
import org.exoplatform.addons.es.client.ElasticClientAuthenticationException;
import org.exoplatform.addons.es.client.ElasticIndexingAuditTrail;
import org.exoplatform.addons.es.client.ElasticIndexingClient;
import org.exoplatform.addons.es.client.ElasticSearchingClient;
import org.exoplatform.addons.es.search.ElasticSearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.exoplatform.services.security.ConversationState;
import org.exoplatform.services.security.Identity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 10/1/15
 */
public class ElasticBasicAuthenticationIntegrationTest extends AbstractIntegrationTest {
  private static final String ES_USERNAME = "esuser";
  private static final String ES_PASSWORD = "espassword";

  private ElasticSearchServiceConnector elasticSearchServiceConnector;

  @Before
  public void initServices() {
    addBasicAuthenticationProperties();
    //Then, override clients with authentication
    elasticSearchingClient = new ElasticSearchingClient(new ElasticIndexingAuditTrail());
    elasticIndexingClient = new ElasticIndexingClient(new ElasticIndexingAuditTrail());

    Identity identity = new Identity("TCL");
    ConversationState.setCurrent(new ConversationState(identity));
    elasticSearchServiceConnector = new ElasticSearchServiceConnector(getInitConnectorParams(), elasticSearchingClient);
  }

  @After
  public void unsetSystemParams() throws InterruptedException, ClassNotFoundException, SQLException {
      removeBasicAuthenticationProperties();
  }

  @Test
  public void testCreateNewIndexWithBasicAuthentication_authenticate_ok() {
    //Given
    assertFalse(indexExists("blog"));
    //When
    elasticIndexingClient.sendCreateIndexRequest("blog", "");
    //Then
    assertTrue(indexExists("blog"));

  }

  @Test(expected = ElasticClientAuthenticationException.class)
  public void testCreateNewIndexWithBasicAuthentication_badUserAuthenticate_NoIndexCreated() {
    //Given
    //Change the username properties to a wrong user
    PropertyManager.setProperty("exo.es.index.server.username", "badUser");
    //Get elasticIndexingClient with latest Properties
    elasticIndexingClient = new ElasticIndexingClient(new ElasticIndexingAuditTrail());
    assertFalse(indexExists("blog"));
    //When
    elasticIndexingClient.sendCreateIndexRequest("blog", "");
    //Then
    fail("ElasticClientAuthenticationException expected");

  }

  @Test(expected = ElasticClientAuthenticationException.class)
  public void testCreateNewIndexWithBasicAuthentication_badPasswordAuthenticate_NoIndexCreated() {
    //Given
    PropertyManager.setProperty("exo.es.index.server.password", "badPassword");
    //Get elasticIndexingClient with latest Properties
    elasticIndexingClient = new ElasticIndexingClient(new ElasticIndexingAuditTrail());
    assertFalse(indexExists("blog"));
    //When
    elasticIndexingClient.sendCreateIndexRequest("blog", "");
    //Then
    fail("ElasticClientAuthenticationException expected");

  }

  @Test
  public void testCreateNewTypeWithBasicAuthentication_authenticate_ok() {
    //Given
    assertFalse(typeExists("post"));
    elasticIndexingClient.sendCreateIndexRequest("blog", "");
    //When
    elasticIndexingClient.sendCreateTypeRequest("blog", "post", "{\"post\" : {}}");
    //Then
    assertTrue(typeExists("post"));

  }

  @Test
  public void testIndexingDocumentWithBasicAuthentication_authenticate_ok() {
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
  public void testSearchingDocumentWithBasicAuthentication_authenticate_ok() {

    //Given
    String mapping = "{ \"properties\" : " + "   {\"permissions\" : "
        + "       {\"type\" : \"string\", \"index\" : \"not_analyzed\"}" + "   }" + "}";
    CreateIndexRequestBuilder cirb = admin().indices().prepareCreate("test").addMapping("type1", mapping);
    cirb.execute().actionGet();
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n" +
        "{ \"field1\" : \"value1\", \"permissions\" : [\"TCL\"] }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
        "{ \"field1\" : \"value2\", \"permissions\" : [\"TCL\"] }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n" +
        "{ \"field1\" : \"value3\", \"permissions\" : [\"TCL\"] }\n";
    elasticIndexingClient.sendCUDRequest(bulkRequest);
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

  @Test(expected = ElasticClientAuthenticationException.class)
  public void testSearchingDocumentWithBasicAuthentication_badUserAuthenticate_searchIsEmpty() {

    //Given
    //Change the username properties to a wrong user
    PropertyManager.setProperty("exo.es.search.server.username", "badUser");
    //Get elasticSearchingClient with latest Properties
    elasticSearchingClient = new ElasticSearchingClient(new ElasticIndexingAuditTrail());
    String mapping = "{ \"properties\" : " + "   {\"permissions\" : "
        + "       {\"type\" : \"string\", \"index\" : \"not_analyzed\"}" + "   }" + "}";
    CreateIndexRequestBuilder cirb = admin().indices().prepareCreate("test").addMapping("type1", mapping);
    cirb.execute().actionGet();
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n" +
        "{ \"field1\" : \"value1\", \"permissions\" : [\"TCL\"] }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
        "{ \"field1\" : \"value2\", \"permissions\" : [\"TCL\"] }\n" +
        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n" +
        "{ \"field1\" : \"value3\", \"permissions\" : [\"TCL\"] }\n";
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    admin().indices().prepareRefresh().execute().actionGet();
    elasticSearchServiceConnector = new ElasticSearchServiceConnector(getInitConnectorParams(), elasticSearchingClient);

    // When
    List<SearchResult> searchResults = new ArrayList<>(elasticSearchServiceConnector.search(null,
        "value1",
        null,
        0,
        10,
        null,
        null));

    // Then
    fail("ElasticClientAuthenticationException expected");

  }

  /**
   * Configuration of the ES integration tests with Basic Authentication plugin activated
   */
  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return ImmutableSettings.settingsBuilder()
        .put(super.nodeSettings(nodeOrdinal))
        .put(RestController.HTTP_JSON_ENABLE, true)
        .put(InternalNode.HTTP_ENABLED, true)
        .put("network.host", "127.0.0.1")
        .put("path.data", "target/data")
        .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
        .put("http.basic.enabled", true)
        .put("http.basic.ipwhitelist", false)
        .put("http.basic.user", ES_USERNAME)
        .put("http.basic.password", ES_PASSWORD)
        .build();
  }

  private InitParams getInitConnectorParams() {
    InitParams params = new InitParams();
    PropertiesParam constructorParams = new PropertiesParam();
    constructorParams.setName("constructor.params");
    constructorParams.setProperty("searchType", "type1");
    constructorParams.setProperty("displayName", "test");
    constructorParams.setProperty("index", "test");
    constructorParams.setProperty("searchFields", "field1");
    params.addParam(constructorParams);
    return params;
  }

  private void addBasicAuthenticationProperties() {
    PropertyManager.setProperty("exo.es.index.server.username",ES_USERNAME);
    PropertyManager.setProperty("exo.es.index.server.password",ES_PASSWORD);
    PropertyManager.setProperty("exo.es.search.server.username", ES_USERNAME);
    PropertyManager.setProperty("exo.es.search.server.password",ES_PASSWORD);
  }

  private void removeBasicAuthenticationProperties() {
    System.clearProperty("exo.es.index.server.username");
    System.clearProperty("exo.es.index.server.password");
    System.clearProperty("exo.es.search.server.username");
    System.clearProperty("exo.es.search.server.password");
  }

}

