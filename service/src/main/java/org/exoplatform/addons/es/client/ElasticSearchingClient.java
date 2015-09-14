package org.exoplatform.addons.es.client;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

/**
 * Created by The eXo Platform SAS Author : eXoPlatform exo@exoplatform.com
 * 9/14/15
 */
public class ElasticSearchingClient {
    private static final Log LOG = ExoLogger.getLogger(ElasticSearchingClient.class);

    private static final String ES_SEARCH_CLIENT_PROPERTY_NAME = "exo.es.search.client";
    private static final String ES_SEARCH_CLIENT_DEFAULT = "http://127.0.0.1:9200";

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
      HttpClient client = new DefaultHttpClient();
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
}
