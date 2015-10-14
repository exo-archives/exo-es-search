package org.exoplatform.addons.es.client;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by The eXo Platform SAS Author : eXoPlatform exo@exoplatform.com
 * 9/14/15
 */
public class ElasticSearchingClient {
    private static final Log LOG = ExoLogger.getLogger(ElasticSearchingClient.class);

    private static final String ES_SEARCH_CLIENT_PROPERTY_NAME = "exo.es.search.server.url";
    private static final String ES_SEARCH_CLIENT_DEFAULT = "http://127.0.0.1:9200";

  private static final String ES_SEARCH_CLIENT_PROPERTY_USERNAME = "exo.es.search.server.username";
  private static final String ES_SEARCH_CLIENT_PROPERTY_PASSWORD = "exo.es.search.server.password";

    //ES Information
    private String urlClient = ES_SEARCH_CLIENT_DEFAULT;

    public ElasticSearchingClient() {
        if (StringUtils.isNotBlank(PropertyManager.getProperty(ES_SEARCH_CLIENT_PROPERTY_NAME))) {
            this.urlClient = PropertyManager.getProperty(ES_SEARCH_CLIENT_PROPERTY_NAME);
        }
    }

    //For testing TODO remove ?
    public ElasticSearchingClient(String urlClient) {
        this.urlClient = urlClient;
    }

    public String sendRequest(String esQuery, String index, String type) {
    String jsonResponse;

    try {
      HttpClient client = getHttpClient();
      HttpPost request = new HttpPost(urlClient + "/" + index + "/" + type + "/_search");
      LOG.info("Search URL request to ES : {}", request.getURI());
      StringEntity input = new StringEntity(esQuery);
      request.setEntity(input);

      HttpResponse response = client.execute(request);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, "UTF-8");
      jsonResponse = writer.toString();
    } catch (IOException e) {
        throw new ElasticClientException(e);
    }

    LOG.debug("ES JSON Response: {}", jsonResponse);

    return jsonResponse;

  }

  private HttpClient getHttpClient() {
    //Check if Basic Authentication need to be used
    if (StringUtils.isNotBlank(PropertyManager.getProperty(ES_SEARCH_CLIENT_PROPERTY_USERNAME))) {
      DefaultHttpClient httpClient = new DefaultHttpClient();
      httpClient.getCredentialsProvider().setCredentials(
          new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT),
          new UsernamePasswordCredentials(PropertyManager.getProperty(ES_SEARCH_CLIENT_PROPERTY_USERNAME)
              , PropertyManager.getProperty(ES_SEARCH_CLIENT_PROPERTY_PASSWORD)));
      LOG.debug("Basic authentication for ES activated");
      return httpClient;
    } else {
      return new DefaultHttpClient();
    }
  }

}
