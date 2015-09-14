package org.exoplatform.addons.es.search;

import org.exoplatform.addons.es.client.ElasticSearchingClient;
import org.exoplatform.container.xml.InitParams;

/**
 * Created by The eXo Platform SAS
 * Author : eXoPlatform
 * exo@exoplatform.com
 * 9/9/15
 */
public class MyElasticSearchServiceConnector extends ElasticSearchServiceConnector {
    public MyElasticSearchServiceConnector(InitParams initParams) {
        super(initParams, new ElasticSearchingClient());
    }
}
