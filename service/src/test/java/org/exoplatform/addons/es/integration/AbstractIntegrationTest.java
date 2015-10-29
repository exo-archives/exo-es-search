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

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.exoplatform.addons.es.client.ElasticIndexingAuditTrail;
import org.exoplatform.commons.utils.PropertyManager;
import org.junit.After;
import org.junit.Before;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.exoplatform.addons.es.client.ElasticIndexingClient;
import org.exoplatform.addons.es.client.ElasticSearchingClient;

/**
 * Created by The eXo Platform SAS Author : Thibault Clement
 * tclement@exoplatform.com 9/11/15
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 0)
public class AbstractIntegrationTest extends ElasticsearchIntegrationTest {

  protected ElasticIndexingClient elasticIndexingClient;
  protected ElasticSearchingClient elasticSearchingClient;
  private boolean propertiesSet;

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
  public void init() {
    internalCluster().ensureAtLeastNumDataNodes(2);
    assertEquals("All nodes in cluster should have HTTP endpoint exposed", 2, cluster().httpAddresses().length);
    String url = "http://" + cluster().httpAddresses()[0].getHostName() + ":" + cluster().httpAddresses()[0].getPort();

    // If the test is executed standalone, the properties are not present at the
    // beginning
    // of the test, so they have to be removed at the end of the test to avoid
    // java.lang.AssertionError: System properties invariant violated from
    // com.carrotsearch.randomizedtesting
    // If another test ran before and set the properties, we must not remove the
    // properties
    // If we do, we'll get the same exception.
    this.propertiesSet = StringUtils.isBlank(System.getProperty("exo.es.index.server.url"));
    PropertyManager.setProperty("exo.es.index.server.url", url);
    PropertyManager.setProperty("exo.es.search.server.url", url);

    elasticIndexingClient = new ElasticIndexingClient(new ElasticIndexingAuditTrail());
    elasticSearchingClient = new ElasticSearchingClient();
  }

  @After
  public void cleanUrlProperties() {
    if (this.propertiesSet) {
      System.clearProperty("exo.es.index.server.url");
      System.clearProperty("exo.es.indexing.batch.number");
      System.clearProperty("exo.es.indexing.replica.number.default");
      System.clearProperty("exo.es.indexing.shard.number.default");
      System.clearProperty("exo.es.search.server.url");
      System.clearProperty("jboss.i18n.generate-proxies");
    } else {
      //Reset server.url properties to default values
      PropertyManager.setProperty("exo.es.index.server.url", "http://127.0.0.1:9200");
      PropertyManager.setProperty("exo.es.search.server.url", "http://127.0.0.1:9200");
    }
  }

  /*
   * Helper class for test Elastic
   */

  protected Boolean typeExists(String index, String type) {
    return client().admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists();
  }

  protected Boolean typeExists(String type) {
    return typeExists("_all", type);
  }

  protected long typeDocumentNumber(String type) {
    CountResponse countResponse = client().prepareCount("_all").setQuery(termQuery("_type", type)).execute().actionGet();
    return countResponse.getCount();
  }

  protected long elasticDocumentNumber() {
    CountResponse countResponse = client().prepareCount().execute().actionGet();
    return countResponse.getCount();
  }

}
