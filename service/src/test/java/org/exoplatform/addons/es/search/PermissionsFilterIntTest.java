package org.exoplatform.addons.es.search;

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
import org.exoplatform.addons.es.integration.AbstractIntegrationTest;
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
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by The eXo Platform SAS
 * Author : eXoPlatform
 * exo@exoplatform.com
 * 9/9/15
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class PermissionsFilterIntTest extends AbstractIntegrationTest {
    private static Connection conn;
    private static Liquibase liquibase;
    private IndexingService indexingService;
    private ElasticSearchServiceConnector elasticSearchServiceConnector;
    private IndexingOperationDAO dao;
    private ElasticIndexingServiceConnector wikiConnector;
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
        //If the test is executed standalone, the properties are not present at the beginning
        //of the test, so they have to be removed at the end of the test to avoid
        // java.lang.AssertionError: System properties invariant violated from com.carrotsearch.randomizedtesting
        //If another test ran before and set the properties, we must not remove the properties
        //If we do, we'll get the same exception.
        this.propertiesSet = StringUtils.isBlank(System.getProperty("exo.es.index.client"));

        //ES URL
        String url = "http://" + cluster().httpAddresses()[0].getHostName() + ":" + cluster().httpAddresses()[0].getPort();

        //Indexing Connector
        wikiConnector = mock(ElasticIndexingServiceConnector.class);
        when(wikiConnector.getType()).thenReturn("wiki");
        when(wikiConnector.getIndex()).thenReturn("wiki");
        when(wikiConnector.getShards()).thenReturn(1);
        when(wikiConnector.getReplicas()).thenReturn(1);
      when(wikiConnector.getMapping()).thenCallRealMethod();
        //IndexService
        dao = new IndexingOperationDAOImpl();
        ElasticIndexingClient client = new ElasticIndexingClient(url);
        ElasticContentRequestBuilder builder = new ElasticContentRequestBuilder();
        indexingService = new ElasticIndexingService(dao, client, builder);
        indexingService.addConnector(wikiConnector);

        //Search connector
        ElasticSearchingClient searchingClient = new ElasticSearchingClient(url);
        elasticSearchServiceConnector = new ElasticSearchServiceConnector(getInitConnectorParams(), searchingClient);
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
        constructorParams.setProperty("searchType", "wiki");
        constructorParams.setProperty("displayName", "wiki");
        constructorParams.setProperty("index", "wiki");
        constructorParams.setProperty("type", "wiki");
        constructorParams.setProperty("searchFields", "title");
        params.addParam(constructorParams);
        return params;
    }

    @Test
    public void test_search_returnsAlicePage() throws IOException, InterruptedException {
        //Given
        setCurrentIdentity("Alice", "admin:/portal");
        dao.create(new IndexingOperation(null, "1", "wiki", OperationType.CREATE, new Date()));
        Document document = new Document();
        document.addField("title", "RDBMS Guidelines");
        document.setPermissions(new String[]{"Alice"});
        document.setId("1");
        when(wikiConnector.create("1")).thenReturn(document);
        indexingService.process();
        admin().indices().prepareRefresh().execute().actionGet();
        //When
        Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "RDBMS", null, 0, 20, null, null);
        //Then
        assertThat(pages.size(), is(1));
    }

    @Test
    public void test_search_doesntReturnBobPage() throws IOException, InterruptedException {
        //Given
        setCurrentIdentity("Alice", "admin:/portal");
        dao.create(new IndexingOperation(null, "1", "wiki", OperationType.CREATE, new Date()));
        Document document = new Document();
        document.addField("title", "RDBMS Guidelines");
        document.setPermissions(new String[]{"Bob"});
        document.setId("1");
        when(wikiConnector.create("1")).thenReturn(document);
        indexingService.process();
        admin().indices().prepareRefresh().execute().actionGet();
        //When
        Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "RDBMS", null, 0, 20, null, null);
        //Then
        assertThat(pages.size(), is(0));
    }

    @Test
    public void test_searchWithMembership_returnsPage() throws IOException, InterruptedException {
        //Given
        setCurrentIdentity("JaneDoe", "publisher:/developers");
        dao.create(new IndexingOperation(null, "1", "wiki", OperationType.CREATE, new Date()));
        Document document = new Document();
        document.addField("title", "RDBMS Guidelines");
        document.setPermissions(new String[]{"Bob", "Alice", "publisher:/developers"});
        document.setId("1");
        when(wikiConnector.create("1")).thenReturn(document);
        indexingService.process();
        admin().indices().prepareRefresh().execute().actionGet();
        //When
        Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "RDBMS", null, 0, 20, null, null);
        //Then
        assertThat(pages.size(), is(1));
    }

    @Test
    public void test_membership_dev_dev() throws IOException, InterruptedException {
        test_searchWithSingleMembership_returnsPage("dev:/pub", "dev:/pub", true);
    }

    @Test
    public void test_membership_star_dev() throws IOException, InterruptedException {
        test_searchWithSingleMembership_returnsPage("*:/pub", "dev:/pub", true);
    }

    @Test
    public void test_membership_dev_star() throws IOException, InterruptedException {
        test_searchWithSingleMembership_returnsPage("dev:/pub", "*:/pub", true);
    }

    @Test
    public void test_membership_xxx_dev() throws IOException, InterruptedException {
        test_searchWithSingleMembership_returnsPage("xxx:/pub", "dev:/pub", false);
    }

    @Test
    public void test_membership_dev_xxx() throws IOException, InterruptedException {
        test_searchWithSingleMembership_returnsPage("dev:/pub", "xxx:/pub", false);
    }

    private void test_searchWithSingleMembership_returnsPage(String membership, String permission, boolean docFound)
    throws IOException, InterruptedException {
        //Given
        setCurrentIdentity("Alice", membership);
        dao.create(new IndexingOperation(null, "1", "wiki", OperationType.CREATE, new Date()));
        Document document = new Document();
        document.addField("title", "RDBMS Guidelines");
        document.setPermissions(new String[]{permission});
        document.setId("1");
        when(wikiConnector.create("1")).thenReturn(document);
        indexingService.process();
        admin().indices().prepareRefresh().execute().actionGet();
        //When
        Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "RDBMS", null, 0, 20, null, null);
        //Then
        assertThat(pages.size(), is(docFound?1:0));
    }

    @Test
    public void test_moreThanOneMembership_found() throws IOException, InterruptedException {
        test_moreThanOneMembership("dev:/pub", true, "dev:/pub", "*:/users", "*:/admin");
    }

    @Test
    public void test_moreThanOneMembership_notfound() throws IOException, InterruptedException {
        test_moreThanOneMembership("admin:/pub", false, "dev:/pub", "*:/users", "*:/admin");
    }

    @Test
    public void test_moreThanOnePermission_found() throws IOException, InterruptedException {
        test_moreThanOnePermission("dev:/pub", true, "dev:/pub", "*:/users", "*:/admin");
    }

    @Test
    public void test_moreThanOnePermission_notfound() throws IOException, InterruptedException {
        test_moreThanOnePermission("admin:/pub", false, "dev:/pub", "*:/users", "*:/admin");
    }

    private void test_moreThanOneMembership(String permission, boolean docFound, String... membership)
            throws IOException, InterruptedException {
        //Given
        setCurrentIdentity("JaneDoe", membership);
        dao.create(new IndexingOperation(null, "1", "wiki", OperationType.CREATE, new Date()));
        Document document = new Document();
        document.addField("title", "RDBMS Guidelines");
        document.setPermissions(new String[]{permission});
        document.setId("1");
        when(wikiConnector.create("1")).thenReturn(document);
        indexingService.process();
        admin().indices().prepareRefresh().execute().actionGet();
        //When
        Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "RDBMS", null, 0, 20, null, null);
        //Then
        assertThat(pages.size(), is(docFound?1:0));
    }

    private void test_moreThanOnePermission(String membership, boolean docFound, String... permissions)
            throws IOException, InterruptedException {
        //Given
        setCurrentIdentity("JaneDoe", membership);
        dao.create(new IndexingOperation(null, "1", "wiki", OperationType.CREATE, new Date()));
        Document document = new Document();
        document.addField("title", "RDBMS Guidelines");
        document.setPermissions(permissions);
        document.setId("1");
        when(wikiConnector.create("1")).thenReturn(document);
        indexingService.process();
        admin().indices().prepareRefresh().execute().actionGet();
        //When
        Collection<SearchResult> pages = elasticSearchServiceConnector.search(null, "RDBMS", null, 0, 20, null, null);
        //Then
        assertThat(pages.size(), is(docFound?1:0));
    }

}
