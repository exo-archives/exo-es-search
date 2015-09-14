package org.exoplatform.addons.es.search;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.json.simple.parser.ParseException;
import org.junit.Test;

import org.exoplatform.addons.es.client.ElasticSearchingClient;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.exoplatform.services.security.ConversationState;
import org.exoplatform.services.security.Identity;
import org.exoplatform.services.security.MembershipEntry;

/**
 * Created by The eXo Platform SAS
 * Author : eXoPlatform
 * exo@exoplatform.com
 * 9/9/15
 */
public class ElasticSearchServiceConnectorTest {
    @Test
    public void testMembership() throws ParseException {
        //Given
        setCurrentIdentity();
        ElasticSearchServiceConnector connector = new ElasticSearchServiceConnector(getInitParams(), new ElasticSearchingClient());
        //When
        String query = connector.buildQuery("My Wiki", 0, 20, "name", "asc");
        //Then
        assertThat(query, containsString("{\"term\" : { \"permissions\" : \"BCH\" }}"));
        assertThat(query, containsString("{\"terms\" : { \"permissions\" : [\".*:Admin\" ]}}"));
    }

    @Test
    public void testSortIsRelevancyByDefault() {
        //Given
        setCurrentIdentity();
        ElasticSearchServiceConnector connector = new ElasticSearchServiceConnector(getInitParams(), new ElasticSearchingClient());
        //When
        String query = connector.buildQuery("My Wiki", 0, 20, null, null);
        //Then
        assertThat(query, containsString("{ \"_score\" : {\"order\" : \"asc\"}}"));
    }

    @Test
    public void testOrderIsAscByDefault() {
        //Given
        setCurrentIdentity();
        ElasticSearchServiceConnector connector = new ElasticSearchServiceConnector(getInitParams(), new ElasticSearchingClient());
        //When
        String query = connector.buildQuery("My Wiki", 0, 20, "name", null);
        //Then
        assertThat(query, containsString("\"sort\""));
        assertThat(query, containsString("{\"order\" : \"asc\"}"));
    }

    @Test
    public void testScoresAreTracked() {
        //Given
        setCurrentIdentity();
        ElasticSearchServiceConnector connector = new ElasticSearchServiceConnector(getInitParams(), new ElasticSearchingClient());
        //When
        String query = connector.buildQuery("My Wiki", 0, 20, "name", "asc");
        //Then
        assertThat(query, containsString("\"track_scores\": true"));
    }

    @Test(expected = IllegalStateException.class)
    public void testSearchWithoutIdentity() {
        //Given
        ConversationState.setCurrent(null);
        ElasticSearchServiceConnector connector = new ElasticSearchServiceConnector(getInitParams(), new ElasticSearchingClient());
        //When
        connector.buildQuery("My Wiki", 0, 20, null, null);
        //Then
        fail("IllegalStateException 'No identity found' expected");
    }

    private InitParams getInitParams() {
        InitParams params = new InitParams();
        PropertiesParam constructorParams = new PropertiesParam();
        constructorParams.setName("constructor.params");
        constructorParams.setProperty("searchType", "wiki");
        constructorParams.setProperty("displayName", "wiki");
        constructorParams.setProperty("index", "wiki");
        constructorParams.setProperty("type", "wiki");
        constructorParams.setProperty("searchFields", "name");
        params.addParam(constructorParams);
        return params;
    }

    private void setCurrentIdentity() {
        Identity identity = new Identity("BCH");
        identity.setMemberships(Arrays.asList(new MembershipEntry("Admin")));
        ConversationState.setCurrent(new ConversationState(identity));
    }
}
