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
package org.exoplatform.addons.es.search;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.commons.api.search.SearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchContext;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.services.security.ConversationState;
import org.exoplatform.services.security.MembershipEntry;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/30/15
 */
public abstract class ElasticSearchServiceConnector extends SearchServiceConnector {

  private static final Log LOG = ExoLogger.getLogger(ElasticSearchServiceConnector.class);

  private static final String ES_SEARCH_CLIENT_PROPERTY_NAME = "exo.es.search.client";
  private static final String ES_SEARCH_CLIENT_DEFAULT = "http://127.0.0.1:9200";

  //ES Information
  private String urlClient = ES_SEARCH_CLIENT_DEFAULT;

  //ES connector information
  private String index;
  private String type;
  private List<String> searchFields;

  //SearchResult information
  private String img;
  private String titleElasticFieldName = "title";
  private String descElasticFieldName = "description";

  private Map<String, String> sortMapping = new HashMap<String, String>();

  public ElasticSearchServiceConnector(InitParams initParams) {
    super(initParams);
    PropertiesParam param = initParams.getPropertiesParam("constructor.params");
    this.index = param.getProperty("index");
    this.type = param.getProperty("type");
    this.searchFields = new ArrayList<>(Arrays.asList(param.getProperty("fields").split(",")));
    //Indicate in which order element will be displayed
    sortMapping.put("relevancy", "_score");
    sortMapping.put("date", "createdDate");
    if (StringUtils.isNotBlank(PropertyManager.getProperty(ES_SEARCH_CLIENT_PROPERTY_NAME)))
      this.urlClient = PropertyManager.getProperty(ES_SEARCH_CLIENT_PROPERTY_NAME);
  }

  @Override
  public Collection<SearchResult> search(SearchContext context, String query, Collection<String> sites,
                                         int offset, int limit, String sort, String order) {
    //TODO sort as an enum ? or a check that the sort is part of the declared fields (in buildQuery also)
    String esQuery = buildQuery(query, offset, limit, sort, order);

    String jsonResponse = sendRequest(esQuery);

    return buildResult(jsonResponse);

  }

  public String buildQuery(String query, int offset, int limit, String sort, String order) {

    StringBuilder esQuery = new StringBuilder();
    esQuery.append("{\n");
    esQuery.append("     \"from\" : " + offset + ", \"size\" : " + limit + ",\n");

    //Score are always tracked, even with sort
    //https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html#_track_scores
    esQuery.append("     \"track_scores\": true,\n");
    esQuery.append("     \"sort\" : [\n");
    esQuery.append("       { \"" + (StringUtils.isNotBlank(sortMapping.get(sort))?sort:"_score") + "\" : ");
    esQuery.append(             "{\"order\" : \"" + (StringUtils.isNotBlank(order)?order:"asc") + "\"}}\n");
    esQuery.append("     ],\n");

    esQuery.append("     \"query\": {\n");
    esQuery.append("        \"filtered\" : {\n");
    esQuery.append("            \"query\" : {\n");
    esQuery.append("                \"query_string\" : {\n");
    esQuery.append("                    \"fields\" : [" + getFields() + "],\n");
    esQuery.append("                    \"query\" : \"" + query + "\"\n");
    esQuery.append("                }\n");
    esQuery.append("            },\n");
    esQuery.append("            \"filter\" : {\n");
    esQuery.append("               \"bool\" : { ");
    esQuery.append("                  \"should\" : [ " + getPermissionFilter() + " ]\n");
    esQuery.append("               }\n");
    esQuery.append("            }\n");
    esQuery.append("        }\n");
    esQuery.append("     },\n");
    esQuery.append("     \"highlight\" : {\n");
    esQuery.append("       \"fields\" : {\n");
    esQuery.append("         \"text\" : {\"fragment_size\" : 150, \"number_of_fragments\" : 3}\n");
    esQuery.append("       }\n");
    esQuery.append("     }\n");
    esQuery.append("}");

    LOG.debug("Search Query request to ES : {} ", esQuery);

    return esQuery.toString();
  }

  private String sendRequest(String esQuery) {

    String jsonResponse = "";

    try {
      HttpClient client = new DefaultHttpClient();
      HttpPost request = new HttpPost(urlClient + "/" + index + "/"
          + type + "/_search");
      LOG.info("Search URL request to ES : {}", request.getURI());
      StringEntity input = new StringEntity(esQuery);
      request.setEntity(input);

      HttpResponse response = client.execute(request);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, "UTF-8");
      jsonResponse = writer.toString();

    } catch (ClientProtocolException e) {
      e.printStackTrace(); //TODO
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace(); //TODO
    } catch (IOException e) {
      e.printStackTrace(); //TODO
    }

    LOG.debug("ES JSON Response: {}", jsonResponse);

    return jsonResponse;

  }

  private Collection<SearchResult> buildResult(String jsonResponse) {

    Collection<SearchResult> results = new ArrayList<SearchResult>();

    JSONParser parser = new JSONParser();

    Map json = null;
    try {
      json = (Map)parser.parse(jsonResponse);
    } catch (ParseException e) {
      e.printStackTrace(); //TODO
    }

    //TODO check if response is successful
    JSONObject jsonResult = (JSONObject) json.get("hits");
    JSONArray jsonHits = (JSONArray) jsonResult.get("hits");

    for(Object jsonHit : jsonHits) {
      JSONObject hitSource = (JSONObject) ((JSONObject) jsonHit).get("_source");
      String title = (String) hitSource.get(titleElasticFieldName);
      String description = (String) hitSource.get(descElasticFieldName);
      String url = (String) hitSource.get("url");
      Long createdDate = (Long) hitSource.get("createdDate");
      Double score = (Double) ((JSONObject) jsonHit).get("_score");

      results.add(new SearchResult(
          url,
          title,
          description,
          description,
          img,
          createdDate,
          //score must not be null as "track_scores" is part of the query
          score.longValue()
      ));
    }

    return results;

  }

  private String getFields() {
    List<String> fields = new ArrayList<>();
    for (String searchField: searchFields) {
      fields.add("\"" + searchField + "\"");
    }
    return StringUtils.join(fields, ",");
  }

  private String getPermissionFilter() {
    String memberships = "";
    Set<String> membershipSet = getUserMemberships();
    if (membershipSet != null) {
      for (String membership : membershipSet) {
        memberships += "\"" + membership + "\"";
      }
      return "{\"term\" : { \"permissions\" : \"" + getCurrentUser() + "\" }}," +
          "{\"terms\" : { \"permissions\" : [" + memberships + " ]}}";
    }
    else {
      return "{\"term\" : { \"permissions\" : \"" + getCurrentUser() + "\" }}";
    }
  }

  public String getCurrentUser() {
    ConversationState conversationState = ConversationState.getCurrent();
    if (conversationState != null) {
      return ConversationState.getCurrent().getIdentity().getUserId();
    }
    return null;
  }

  private Set<String> getUserMemberships() {

    ConversationState conversationState = ConversationState.getCurrent();

    if (conversationState != null) {
      Set<String> entries = new HashSet<String>();
      for (MembershipEntry entry : ConversationState.getCurrent().getIdentity().getMemberships()) {
        //If it's a wildcard membership, add a point to transform it to regexp
        if (entry.getMembershipType().equals(MembershipEntry.ANY_TYPE)) {
          entries.add(entry.toString().replace("*", ".*"));
        }
        //If it's not a wildcard membership
        else {
          //Add the membership
          entries.add(entry.toString());
          //Also add a wildcard membership (not as a regexp) in order to match to wildcard permission
          //Ex: membership dev:/pub must match permission dev:/pub and permission *:/pub
          entries.add("*:"+entry.getGroup());
        }
      }
      return entries;
    }
    return null;
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

  public String getImg() {
    return img;
  }

  public void setImg(String img) {
    this.img = img;
  }

  public String getDescElasticFieldName() {
    return descElasticFieldName;
  }

  public void setDescElasticFieldName(String descElasticFieldName) {
    this.descElasticFieldName = descElasticFieldName;
  }

  public String getTitleElasticFieldName() {
    return titleElasticFieldName;
  }

  public void setTitleElasticFieldName(String titleElasticFieldName) {
    this.titleElasticFieldName = titleElasticFieldName;
  }

  public List<String> getSearchFields() {
    return searchFields;
  }

  public void setSearchFields(List<String> searchFields) {
    this.searchFields = searchFields;
  }
}

