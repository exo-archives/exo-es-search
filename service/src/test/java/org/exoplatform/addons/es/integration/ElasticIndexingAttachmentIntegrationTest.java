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
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestController;
import org.junit.Before;
import org.junit.Test;

import org.exoplatform.addons.es.search.ElasticSearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.exoplatform.services.security.ConversationState;
import org.exoplatform.services.security.Identity;
import org.exoplatform.services.security.MembershipEntry;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 10/1/15
 */
public class ElasticIndexingAttachmentIntegrationTest extends AbstractIntegrationTest {

  //"Some people want it to happen, some wish it would happen, others make it happen." sentence encoded in base 64
  private final static String MC23Quotes =
      "U29tZSBwZW9wbGUgd2FudCBpdCB0byBoYXBwZW4sIHNvbWUgd2lzaCBpdCB3b3VsZCBoYXBwZW4sIG90aGVycyBtYWtlIGl0IGhhcHBlbi4=";

  private ElasticSearchServiceConnector elasticSearchServiceConnector;

  @Before
  public void initServices() {
    Identity identity = new Identity("TCL");
    identity.setMemberships(Collections.singletonList(new MembershipEntry("BasketballPlayer")));
    ConversationState.setCurrent(new ConversationState(identity));
    elasticSearchServiceConnector = new ElasticSearchServiceConnector(getInitConnectorParams(), elasticSearchingClient);
  }

  private InitParams getInitConnectorParams() {
    InitParams params = new InitParams();
    PropertiesParam constructorParams = new PropertiesParam();
    constructorParams.setName("constructor.params");
    constructorParams.setProperty("searchType", "attachment");
    constructorParams.setProperty("displayName", "attachment");
    constructorParams.setProperty("index", "test");
    constructorParams.setProperty("searchFields", "file,title");
    params.addParam(constructorParams);
    return params;
  }

  /**
   * Configuration of the ES integration tests with
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
        .build();
  }

  @Test
  public void testCreateNewAttachmentType() {
    //Given
    assertFalse(typeExists("attachment"));
    elasticIndexingClient.sendCreateIndexRequest("test", "");
    //When
    elasticIndexingClient.sendCreateTypeRequest("test", "attachment", getAttachmentMapping());
    //Then
    assertTrue(typeExists("attachment"));

  }

  @Test
  public void testIndexAttachment() {
    //Given
    createAttachmentTypeInTestIndex();
    assertEquals(0,elasticDocumentNumber());
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"attachment\", \"_id\" : \"1\" } }\n" +
        "{ " +
        "\"title\" : \"Sample CV in English\"," +
        "\"file\" : \" " + MC23Quotes + " \"" +
        " }\n";

    //When
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    admin().indices().prepareRefresh().execute().actionGet();

    //Then
    assertEquals(1,elasticDocumentNumber());

  }

  @Test
  public void testSearchAttachment() {
    //Given
    createAttachmentTypeInTestIndex();
    assertEquals(0,elasticDocumentNumber());
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"attachment\", \"_id\" : \"1\" } }\n" +
        "{ " +
        "\"title\" : \"Michael Jordan quotes\", " +
        "\"file\" : \""+ MC23Quotes + "\", " +
        "\"permissions\" : [\"TCL\"]" +
        " }\n";

    //When
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    admin().indices().prepareRefresh().execute().actionGet();
    List<SearchResult> searchResults = new ArrayList<>(elasticSearchServiceConnector.search(null,
        "people",
        null,
        0,
        10,
        null,
        null));

    //Then
    assertEquals("... Some <strong>people</strong> want it to happen, some wish it would happen, others make it happen.\n", searchResults.get(0).getExcerpt());

  }

  private void createAttachmentTypeInTestIndex() {
    CreateIndexRequestBuilder cirb = admin().indices().prepareCreate("test").addMapping("attachment", getAttachmentMapping());
    cirb.execute().actionGet();
  }

  private String getAttachmentMapping() {
    return "{\"properties\" : {\n" +
        "      \"file\" : {\n" +
        "        \"type\" : \"attachment\",\n" +
        "        \"fields\" : {\n" +
        "          \"title\" : { \"store\" : \"yes\" },\n" +
        "          \"file\" : { \"term_vector\":\"with_positions_offsets\", \"store\":\"yes\" }\n" +
        "        }\n" +
        "      },\n" +
        "      \"permissions\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\" }\n" +
        "    }" +
        "}";
  }

}

