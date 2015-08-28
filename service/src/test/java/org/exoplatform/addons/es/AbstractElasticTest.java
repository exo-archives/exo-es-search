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
package org.exoplatform.addons.es;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.DefaultHttpClient;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.exoplatform.addons.es.blogpost.Blogpost;
import org.exoplatform.addons.es.blogpost.BlogpostElasticSearchConnector;
import org.exoplatform.addons.es.blogpost.BlogpostService;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingService;
import org.exoplatform.addons.es.search.ElasticSearchServiceConnector;
import org.exoplatform.commons.persistence.impl.EntityManagerService;
import org.exoplatform.container.PortalContainer;
import org.exoplatform.container.component.RequestLifeCycle;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.junit.*;

import javax.persistence.EntityManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/27/15
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 0)
public abstract class AbstractElasticTest extends ElasticsearchIntegrationTest {

  protected static final String CONNECTOR_INDEX = "blog";
  protected static final String CONNECTOR_TYPE = "post";

  protected IndexingService indexingService;
  protected BlogpostService blogpostService;
  protected ElasticSearchServiceConnector searchServiceConnector;

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
        .build();
  }

  @Before
  public void waitForNodes() {
    internalCluster().ensureAtLeastNumDataNodes(4);
    assertEquals("All nodes in cluster should have HTTP endpoint exposed", 4, cluster().httpAddresses().length);
    indexingService = new ElasticIndexingService("http://"+cluster().httpAddresses()[0].getHostName()+":"+cluster().httpAddresses()[0].getPort());
  }

  private static Connection conn;
  private static Liquibase liquibase;

  EntityManagerService entityMgrService;

  @BeforeClass
  public static void startDB () throws ClassNotFoundException, SQLException, LiquibaseException {

    Class.forName("org.hsqldb.jdbcDriver");
    conn = DriverManager.getConnection("jdbc:hsqldb:file:target/hsql-db", "sa", "");

    Database database = DatabaseFactory.getInstance()
        .findCorrectDatabaseImplementation(new JdbcConnection(conn));

    //Create Table
    liquibase = new Liquibase("src/main/resources/org/exoplatform/addons/es/db/changelog/db.changelog.xml",
        new FileSystemResourceAccessor(), database);
    liquibase.update((String) null);

  }

  @AfterClass
  public static void stopDB () throws LiquibaseException, SQLException {

    liquibase.rollback(1000, null);
    conn.close();

  }

  @Before
  public void initializeContainerAndStartRequestLifecycle() {
    PortalContainer container = PortalContainer.getInstance();

    RequestLifeCycle.begin(container);

    entityMgrService = container.getComponentInstanceOfType(EntityManagerService.class);
    entityMgrService.getEntityManager().getTransaction().begin();
  }

  @After
  public void endRequestLifecycle() {

    entityMgrService.getEntityManager().getTransaction().commit();

    RequestLifeCycle.end();
  }

  protected EntityManager getEntityManager() {
    return entityMgrService.getEntityManager();
  }

  @Before
  public void initService() {
    PortalContainer container = PortalContainer.getInstance();
    //indexingService = container.getComponentInstanceOfType(IndexingService.class);
    blogpostService = new BlogpostService();
    searchServiceConnector = getBlogPostSearchConnector();
  }

  @After
  public void clean() {
    indexingService.getConnectors().clear();
    indexingService.clearIndexQueue();
    cleanElastic();
  }

  protected BlogpostElasticSearchConnector getBlogPostSearchConnector() {

    PropertiesParam propertiesParam = new PropertiesParam();
    propertiesParam.getProperties().put("index", CONNECTOR_INDEX);
    propertiesParam.getProperties().put("type", CONNECTOR_TYPE);
    propertiesParam.getProperties().put("fields", "title,content,author");
    InitParams initParams = new InitParams();
    initParams.put("constructor.params", propertiesParam);

    return new BlogpostElasticSearchConnector(initParams);
  }

  private void cleanElastic() {
    HttpClient client = new DefaultHttpClient();

    HttpDelete httpDeleteRequest = new HttpDelete("http://127.0.0.1:9200/" + CONNECTOR_INDEX);
  }

  protected Blogpost getDefaultBlogpost() {

    Blogpost deathStartProject = new Blogpost();
    deathStartProject.setId(1L);
    deathStartProject.setAuthor("Thibault");
    deathStartProject.setPostDate(new Date());
    deathStartProject.setTitle("Death Star Project");
    deathStartProject.setContent("The new Death Star Project is under the responsability of the Vador Team : Benoit," +
        " Thibault and Tuyen");
    return deathStartProject;

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
    return typeDocumentNumber("_all", type);
  }

  protected long indexDocumentNumber(String index) {
    CommonStats stats = client().admin().indices().prepareStats(index).execute().actionGet().getPrimaries();
    return stats.getDocs().getCount();
  }

  protected long elasticDocumentNumber() {
    return indexDocumentNumber("_all");
  }

}

