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
import org.exoplatform.addons.es.search.ElasticSearchFilter;
import org.exoplatform.addons.es.search.ElasticSearchFilterType;
import org.exoplatform.addons.es.search.ElasticSearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.exoplatform.services.security.ConversationState;
import org.exoplatform.services.security.Identity;
import org.exoplatform.services.security.MembershipEntry;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 11/24/15
 */
public class ExtendedElasticSearchingIntegrationTest extends AbstractIntegrationTest  {

  ElasticSearchServiceConnector elasticSearchServiceConnector;

  @Before
  public void initServices() {
    Identity identity = new Identity("TCL");
    identity.setMemberships(Collections.singletonList(new MembershipEntry("Admin")));
    ConversationState.setCurrent(new ConversationState(identity));
    elasticSearchServiceConnector = new ElasticSearchServiceConnector(getInitConnectorParams(), elasticSearchingClient);
  }

  private InitParams getInitConnectorParams() {
    InitParams params = new InitParams();
    PropertiesParam constructorParams = new PropertiesParam();
    constructorParams.setName("constructor.params");
    constructorParams.setProperty("searchType", "type1");
    constructorParams.setProperty("type", "type1");
    constructorParams.setProperty("displayName", "test");
    constructorParams.setProperty("index", "test");
    constructorParams.setProperty("searchFields", "field1");
    params.addParam(constructorParams);
    return params;
  }

  @Test
  public void testSearchingDocument_AddAdditionalFilter() {

    // Given
    assertEquals(0, elasticDocumentNumber());
    String mapping = "{ \"properties\" : " + "   {\"permissions\" : "
        + "       {\"type\" : \"string\", \"index\" : \"not_analyzed\"}" + "   }" + "}";
    CreateIndexRequestBuilder cirb = admin().indices().prepareCreate("test").addMapping("type1", mapping);
    cirb.execute().actionGet();
    String bulkRequest = "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n"
        + "{ \"field1\" : \"value1\", \"permissions\" : [\"TCL\"], \"otherFilter\" : \"f1\" }\n"
        + "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n"
        + "{ \"field1\" : \"value1\", \"permissions\" : [\"TCL\"], \"otherFilter\" : \"f1\" }\n"
        + "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n"
        + "{ \"field1\" : \"value1\", \"permissions\" : [\"TCL\"], \"otherFilter\" : \"f2\" }\n";
    elasticIndexingClient.sendCUDRequest(bulkRequest);
    // Elasticsearch has near real-time search: document changes are not visible
    // to search immediately,
    // but will become visible within 1 second
    admin().indices().prepareRefresh().execute().actionGet();
    List<ElasticSearchFilter> filters = new ArrayList<>();
    filters.add(new ElasticSearchFilter(ElasticSearchFilterType.FILTER_BY_TERM, "otherFilter", "f1"));

    // When
    List<SearchResult> searchResults = new ArrayList<>(elasticSearchServiceConnector.filteredSearch(null,
        "value1",
        filters,
        null,
        0,
        10,
        null,
        null));

    // Then
    assertEquals(2, searchResults.size());

  }
  
}

