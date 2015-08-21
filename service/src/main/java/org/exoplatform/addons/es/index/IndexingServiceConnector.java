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
package org.exoplatform.addons.es.index;

import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.json.simple.JSONObject;

import java.util.List;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/22/15
 */
public abstract class IndexingServiceConnector {

  String index;
  String type;
  String mapping;
  Integer shards;
  Integer replicas;

  private static final Integer DEFAULT_REPLICAS_NUMBER = Integer.valueOf((System.getProperty("indexing.replicasNumber.default") != null) ?
      System.getProperty("indexing.replicasNumber.default") : "1");

  private static final Integer DEFAULT_SHARDS_NUMBER = Integer.valueOf((System.getProperty("indexing.shardsNumber.default") != null) ?
      System.getProperty("indexing.shardsNumber.default") : "5");

  public IndexingServiceConnector(InitParams initParams) {
    PropertiesParam param = initParams.getPropertiesParam("constructor.params");
    this.index = param.getProperty("index");
    this.type = param.getProperty("type");
    this.mapping = param.getProperty("mapping");
    this.shards = DEFAULT_SHARDS_NUMBER;
    this.replicas = DEFAULT_REPLICAS_NUMBER;
  }

  /**
   *
   * Initialise the index
   *
   * @return JSON containing index initialisation parameters
   *
   * @LevelAPI Experimental
   */
  public abstract String init();

  /**
   *
   * Transform an entity to Document in order to be indexed
   *
   * @param id Id of entity to index
   *
   * @return List of Document to index
   *
   * @LevelAPI Experimental
   */
  public abstract Document index (String id);

  /**
   *
   * Transform a list of entities to Document in order to be deleted from index
   *
   * @param id Ids of entities to delete from
   *
   * @return List of Ids to delete from index
   *
   * @LevelAPI Experimental
   */
  public abstract String delete (String id);

  /**
   *
   * Transform all entities to Document in order to be deleted from index
   *
   * @return List of Ids to delete from index
   *
   * @LevelAPI Experimental
   */
  public abstract List<String> deleteAll ();

  public String getMappingJSON() {

    if (mapping == null || mapping.isEmpty()) {

      //Return default mapping:
      /*
        {
            "type_name" : {
                "properties" : {
                    "permissions" : {"type" : "string", "index" : not_analyzed }
                }
            }
        }
       */

      JSONObject permissionAttributes = new JSONObject();
      permissionAttributes.put("type", "string");
      permissionAttributes.put("index", "not_analyzed");

      JSONObject permission = new JSONObject();
      permission.put("permissions", permissionAttributes);

      JSONObject mappingProperties = new JSONObject();
      mappingProperties.put("properties",permission);

      JSONObject mappingJSON = new JSONObject();
      mappingJSON.put(type, mappingProperties);

      return mappingJSON.toJSONString();
    }
    //Else return the map[ping provide by developer
    return mapping;
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getMapping() {
    return mapping;
  }

  public void setMapping(String mapping) {
    this.mapping = mapping;
  }

  public Integer getShards() {
    return shards;
  }

  public void setShards(Integer shards) {
    this.shards = shards;
  }

  public Integer getReplicas() {
    return replicas;
  }

  public void setReplicas(Integer replicas) {
    this.replicas = replicas;
  }
}

