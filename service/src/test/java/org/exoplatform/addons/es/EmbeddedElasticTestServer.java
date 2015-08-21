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

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/19/15
 */
public class EmbeddedElasticTestServer {

  private static final String ES_DATA_DIR = "target/es-data";

  private final Node node;

  public EmbeddedElasticTestServer() {

    Settings settings = ImmutableSettings.settingsBuilder()
        .put("http.enabled", "true")
        .put("path.data", ES_DATA_DIR)
        .put("network.host", "127.0.0.1")
        .build();

    node = NodeBuilder.nodeBuilder()
        .local(true)
        .settings(settings).node();
  }

  public void shutdown () {
    node.close();
    //Delete data for next test
    deleteElasticData();
  }

  private void deleteElasticData() {
    try {
      FileUtils.deleteDirectory(new File(ES_DATA_DIR));
    } catch (IOException e) {
      throw new RuntimeException("Impossible to delete elastic server data directory", e);
    }
  }

}

