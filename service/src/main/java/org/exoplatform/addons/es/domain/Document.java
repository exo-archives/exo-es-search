/*
 * Copyright (C) 2015 eXo Platform SAS.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.exoplatform.addons.es.domain;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * @author <a href="mailto:tuyennt@exoplatform.com">Tuyen Nguyen The</a>.
 */
public class Document {
  private String type;
  private String id;
  private String url;
  private Date createdDate;
  private String[] permissions;
  private Map<String, String> fields;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public Date getCreatedDate() {
    return createdDate;
  }

  public void setCreatedDate(Date createdDate) {
    this.createdDate = createdDate;
  }

  public String[] getPermissions() {
    return permissions;
  }

  public void setPermissions(String[] permissions) {
    this.permissions = permissions;
  }

  public Map<String, String> getFields() {
    return fields;
  }

  public void setFields(Map<String, String> fields) {
    this.fields = fields;
  }

  public String toJSON() {
    String json;
    JSONObject obj = new JSONObject();
    JSONArray permissionsJSON = new JSONArray();
    permissionsJSON.addAll(permissions != null ? Arrays.asList(permissions) : Collections.EMPTY_LIST);
    obj.put("permissions", permissionsJSON);
    obj.put("url", url);
    obj.put("createdDate", createdDate.getTime());
    for(String fieldName: fields.keySet()) {
      obj.put(fieldName, fields.get(fieldName));
    }
    json = obj.toJSONString();
    return json;
  }
}
