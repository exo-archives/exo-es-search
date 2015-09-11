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

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.exoplatform.addons.es.client.ElasticIndexingClient;
import org.junit.Before;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/11/15
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class AbstractIntegrationTest  extends ElasticsearchIntegrationTest {

  protected ElasticIndexingClient elasticIndexingClient;

  /**
   * Configuration of the ES integration tests
   */
  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return ImmutableSettings.settingsBuilder()
        .put(super.nodeSettings(nodeOrdinal))
        .put(RestController.HTTP_JSON_ENABLE, true)
        .put(InternalNode.HTTP_ENABLED, true)
        .put("network.host", "127.0.0.1")
        .put("path.data", "target/data")
        .build();
  }

  @Before
  public void waitForNodes() {
    internalCluster().ensureAtLeastNumDataNodes(4);
    assertEquals("All nodes in cluster should have HTTP endpoint exposed", 4, cluster().httpAddresses().length);
    elasticIndexingClient = new ElasticIndexingClient("http://"+cluster().httpAddresses()[0].getHostName()+":"+cluster().httpAddresses()[0].getPort());
  }

  /*
  Helper class for test Elastic
   */

  protected Boolean typeExists(String index, String type) {
    return client().admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists();
  }

  protected Boolean typeExists(String type) {
    return typeExists("_all", type);
  }

  protected long typeDocumentNumber(String index, String type) {
    CommonStats stats = client().admin().indices().prepareStats(index).setTypes(type).execute().actionGet().getPrimaries();
    return stats.getDocs().getCount();
  }

  protected long typeDocumentNumber(String type) {
    CountResponse countResponse = client().prepareCount("_all").setQuery(termQuery("_type", type)).execute().actionGet();
    return countResponse.getCount();
  }

  protected long indexDocumentNumber(String index) {
    CountResponse countResponse = client().prepareCount(index).execute().actionGet();
    return countResponse.getCount();
  }

  protected long elasticDocumentNumber() {
    CountResponse countResponse = client().prepareCount().execute().actionGet();
    return countResponse.getCount();
  }

}

