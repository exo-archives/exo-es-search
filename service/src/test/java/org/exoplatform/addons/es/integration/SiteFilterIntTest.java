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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;
import org.elasticsearch.common.lang3.StringUtils;
import org.exoplatform.addons.es.client.ElasticContentRequestBuilder;
import org.exoplatform.addons.es.client.ElasticIndexingClient;
import org.exoplatform.addons.es.client.ElasticSearchingClient;
import org.exoplatform.addons.es.dao.IndexingOperationDAO;
import org.exoplatform.addons.es.dao.impl.IndexingOperationDAOImpl;
import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.addons.es.domain.IndexingOperation;
import org.exoplatform.addons.es.domain.OperationType;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingService;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingServiceConnector;
import org.exoplatform.addons.es.search.ElasticSearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.exoplatform.services.security.ConversationState;
import org.exoplatform.services.security.Identity;
import org.exoplatform.services.security.MembershipEntry;
import org.junit.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 10/2/15
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SiteFilterIntTest extends AbstractIntegrationTest {

  private static final String USERNAME = "thib";

  private static Connection conn;
  private static Liquibase liquibase;
  private IndexingService indexingService;
  private ElasticSearchServiceConnector elasticSearchServiceConnector;
  private IndexingOperationDAO dao;
  private ElasticIndexingServiceConnector testConnector;
  private boolean propertiesSet;

  @BeforeClass
  public static void startDB () throws ClassNotFoundException, SQLException, LiquibaseException {
    Class.forName("org.hsqldb.jdbcDriver");
    conn = DriverManager.getConnection("jdbc:hsqldb:file:target/hsql-db", "sa", "");
    Database database = DatabaseFactory.getInstance()
        .findCorrectDatabaseImplementation(new JdbcConnection(conn));
    //Create Table
    liquibase = new Liquibase("./src/main/resources/db/changelog/exo-search.db.changelog-1.0.0.xml",
        new FileSystemResourceAccessor(), database);
    liquibase.update((String) null);
  }

  @AfterClass
  public static void stopDB () throws LiquibaseException, SQLException {
    liquibase.rollback(1000, null);
    conn.close();
  }

  @Before
  public void initServices() {
    this.propertiesSet = StringUtils.isBlank(System.getProperty("exo.es.index.client"));

    //ES URL
    String url = "http://" + cluster().httpAddresses()[0].getHostName() + ":" + cluster().httpAddresses()[0].getPort();

    //Indexing Connector
    testConnector = mock(ElasticIndexingServiceConnector.class);
    when(testConnector.getType()).thenReturn("test");
    when(testConnector.getIndex()).thenReturn("test");
    when(testConnector.getShards()).thenReturn(1);
    when(testConnector.getReplicas()).thenReturn(1);
    when(testConnector.getMapping()).thenCallRealMethod();
    //IndexService
    dao = new IndexingOperationDAOImpl();
    ElasticIndexingClient client = new ElasticIndexingClient(url);
    ElasticContentRequestBuilder builder = new ElasticContentRequestBuilder();
    indexingService = new ElasticIndexingService(dao, client, builder);
    indexingService.addConnector(testConnector);

    //Search connector
    ElasticSearchingClient searchingClient = new ElasticSearchingClient(url);
    elasticSearchServiceConnector = new ElasticSearchServiceConnector(getInitConnectorParams(), searchingClient);

    //Set identity
    setCurrentIdentity(USERNAME);
  }

  private void setCurrentIdentity(String userId, String... memberships) {
    Set<MembershipEntry> membershipEntrySet = new HashSet<>();
    if (memberships!=null) {
      for (String membership : memberships) {
        String[] membershipSplit = membership.split(":");
        membershipEntrySet.add(new MembershipEntry(membershipSplit[1], membershipSplit[0]));
      }
    }
    ConversationState.setCurrent(new ConversationState(new Identity(userId, membershipEntrySet)));
  }

  @After
  public void unsetSystemParams() throws InterruptedException, ClassNotFoundException, SQLException {
    if (propertiesSet) {
      System.clearProperty("exo.es.index.client");
      System.clearProperty("exo.es.indexing.batch.number");
      System.clearProperty("exo.es.indexing.replica.number.default");
      System.clearProperty("exo.es.indexing.shard.number.default");
      System.clearProperty("exo.es.search.client");
      System.clearProperty("jboss.i18n.generate-proxies");
    }
  }

  private InitParams getInitConnectorParams() {
    InitParams params = new InitParams();
    PropertiesParam constructorParams = new PropertiesParam();
    constructorParams.setName("constructor.params");
    constructorParams.setProperty("searchType", "test");
    constructorParams.setProperty("displayName", "test");
    constructorParams.setProperty("index", "test");
    constructorParams.setProperty("type", "test");
    constructorParams.setProperty("searchFields", "title");
    params.addParam(constructorParams);
    return params;
  }

  @Test
  public void test_searchIntranetSite_returnsIntranetDocument() throws IOException, InterruptedException {
    //Given
    dao.create(new IndexingOperation(null, "1", "test", OperationType.CREATE, new Date()));
    when(testConnector.create("1")).thenReturn(getSiteDocument("intranet"));
    indexingService.process();
    admin().indices().prepareRefresh().execute().actionGet();
    //When
    Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "test", getSites(), 0, 20, null, null);
    //Then
    assertThat(pages.size(), is(1));
  }

  @Test
  public void test_searchIntranetSite_returnsNoDocumentAttachToOtherSite() throws IOException, InterruptedException {
    //Given
    dao.create(new IndexingOperation(null, "1", "test", OperationType.CREATE, new Date()));

    when(testConnector.create("1")).thenReturn(getSiteDocument("OtherSite"));
    indexingService.process();
    admin().indices().prepareRefresh().execute().actionGet();
    //When
    Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "test", getSites(), 0, 20, null, null);
    //Then
    assertThat(pages.size(), is(0));
  }

  @Test
  public void test_searchIntranetSite_returnsDocumentNoAttachToSite() throws IOException, InterruptedException {
    //Given
    dao.create(new IndexingOperation(null, "1", "test", OperationType.CREATE, new Date()));
    when(testConnector.create("1")).thenReturn(getSiteDocument(null));
    indexingService.process();
    admin().indices().prepareRefresh().execute().actionGet();
    //When
    Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "test", getSites(), 0, 20, null, null);
    //Then
    assertThat(pages.size(), is(1));
  }

  private Document getSiteDocument(String siteName) {
    Document document = new Document();
    document.addField("title", "A test document");
    if (siteName != null) document.addField("sites", siteName);
    document.setPermissions(new String[]{USERNAME});
    document.setId("1");
    return document;
  }

  private Collection<String> getSites() {
    Collection<String> sites = new ArrayList<>();
    sites.add("intranet");
    return sites;
  }
  
}
