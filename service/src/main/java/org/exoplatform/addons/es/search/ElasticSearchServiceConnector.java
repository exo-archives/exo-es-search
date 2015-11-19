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

import org.apache.commons.lang.StringUtils;
import org.exoplatform.addons.es.client.ElasticSearchingClient;
import org.exoplatform.commons.api.search.SearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchContext;
import org.exoplatform.commons.api.search.data.SearchResult;
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

import java.util.*;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/30/15
 */
public class ElasticSearchServiceConnector extends SearchServiceConnector {
  private static final Log LOG = ExoLogger.getLogger(ElasticSearchServiceConnector.class);
  private final ElasticSearchingClient client;

  //ES connector information
  //Index is optional: if null, search on all the cluster
  private String index;
  //Type is optional: if null, search on all the index
  private String type;
  private List<String> searchFields;

  //SearchResult information
  private String img;
  private String titleElasticFieldName = "title";

  private Map<String, String> sortMapping = new HashMap<>();

  public ElasticSearchServiceConnector(InitParams initParams, ElasticSearchingClient client) {
    super(initParams);
    this.client = client;
    PropertiesParam param = initParams.getPropertiesParam("constructor.params");
    this.index = param.getProperty("index");
    this.type = param.getProperty("type");
    this.titleElasticFieldName = param.getProperty("titleField");
    this.searchFields = new ArrayList<>(Arrays.asList(param.getProperty("searchFields").split(",")));
    //Indicate in which order element will be displayed
    sortMapping.put("relevancy", "_score");
    sortMapping.put("date", "lastUpdatedDate");
  }

  @Override
  public Collection<SearchResult> search(SearchContext context, String query, Collection<String> sites,
                                         int offset, int limit, String sort, String order) {
    String esQuery = buildQuery(query, sites, offset, limit, sort, order);
    String jsonResponse = this.client.sendRequest(esQuery, this.index, this.type);
    return buildResult(jsonResponse);

  }

  public String buildQuery(String query, Collection<String> sites, int offset, int limit, String sort, String order) {
    StringBuilder esQuery = new StringBuilder();
    esQuery.append("{\n");
    esQuery.append("     \"from\" : " + offset + ", \"size\" : " + limit + ",\n");

    //Score are always tracked, even with sort
    //https://www.impl.co/guide/en/elasticsearch/reference/current/search-request-sort.html#_track_scores
    esQuery.append("     \"track_scores\": true,\n");
    esQuery.append("     \"sort\" : [\n");
    esQuery.append("       { \"" + (StringUtils.isNotBlank(sortMapping.get(sort))?sortMapping.get(sort):"_score") + "\" : ");
    esQuery.append(             "{\"order\" : \"" + (StringUtils.isNotBlank(order)?order:"asc") + "\"}}\n");
    esQuery.append("     ],\n");
    esQuery.append("     \"_source\": [" + getSourceFields() + "],");
    esQuery.append("     \"query\": {\n");
    esQuery.append("        \"filtered\" : {\n");
    esQuery.append("            \"query\" : {\n");
    esQuery.append("                \"query_string\" : {\n");
    esQuery.append("                    \"fields\" : [" + getFields() + "],\n");
    esQuery.append("                    \"query\" : \"" + query + "\"\n");
    esQuery.append("                }\n");
    esQuery.append("            },\n");
    esQuery.append("            \"filter\" : {\n");
    esQuery.append("              \"bool\" : {\n");
    esQuery.append("                \"must\" : [\n");
    esQuery.append("                  {\n");
    esQuery.append("                   \"bool\" : {\n");
    esQuery.append("                     \"should\" : [\n");
    esQuery.append("                      " + getPermissionFilter() + "\n");
    esQuery.append("                      ]\n");
    esQuery.append("                    }\n");
    esQuery.append("                  },\n");
    esQuery.append("                  {\n");
    esQuery.append("                   \"bool\" : {\n");
    esQuery.append("                     \"should\" : [\n");
    esQuery.append("                      " + getSitesFilter(sites) + "\n");
    esQuery.append("                       ]\n");
    esQuery.append("                    }\n");
    esQuery.append("                  }\n");
    esQuery.append("                ]\n");
    esQuery.append("              }\n");
    esQuery.append("            }");
    esQuery.append("        }\n");
    esQuery.append("     },\n");
    esQuery.append("     \"highlight\" : {\n");
    esQuery.append("       \"pre_tags\" : [\"<strong>\"],\n");
    esQuery.append("       \"post_tags\" : [\"</strong>\"],\n");
    esQuery.append("       \"fields\" : {\n");
    for (int i=0; i<this.searchFields.size(); i++) {
      esQuery.append("         \""+searchFields.get(i)+"\" : {\"fragment_size\" : 150, \"number_of_fragments\" : 3}");
      if (i<this.searchFields.size()-1) {
        esQuery.append(",");
      }
      esQuery.append("\n");
    }
    esQuery.append("       }\n");
    esQuery.append("     }\n");
    esQuery.append("}");

    LOG.debug("Search Query request to ES : {} ", esQuery);

    return esQuery.toString();
  }

  private Collection<SearchResult> buildResult(String jsonResponse) {

    LOG.debug("Search Query response from ES : {} ", jsonResponse);

    Collection<SearchResult> results = new ArrayList<>();
    JSONParser parser = new JSONParser();

    Map json;
    try {
      json = (Map)parser.parse(jsonResponse);
    } catch (ParseException e) {
      throw new ElasticSearchException("Unable to parse JSON response", e);
    }

    //TODO check if response is successful
    JSONObject jsonResult = (JSONObject) json.get("hits");
    JSONArray jsonHits = (JSONArray) jsonResult.get("hits");

    for(Object jsonHit : jsonHits) {
      JSONObject hitSource = (JSONObject) ((JSONObject) jsonHit).get("_source");
      String title = (String) hitSource.get(titleElasticFieldName);
      String url = (String) hitSource.get("url");
      Long lastUpdatedDate = (Long) hitSource.get("lastUpdatedDate");
      if (lastUpdatedDate == null) lastUpdatedDate = new Date().getTime();
      Double score = (Double) ((JSONObject) jsonHit).get("_score");
      //Get the excerpt
      JSONObject hitHighlight = (JSONObject) ((JSONObject) jsonHit).get("highlight");
      Iterator<?> keys = hitHighlight.keySet().iterator();
      StringBuilder excerpt = new StringBuilder();
      while( keys.hasNext() ) {
        String key = (String)keys.next();
        JSONArray highlights = (JSONArray) hitHighlight.get(key);
        for (Object highlight : highlights) {
          excerpt.append("... ").append(highlight);
        }
      }

      LOG.debug("Excerpt extract from ES response : "+excerpt.toString());

      results.add(new SearchResult(
          url,
          title,
          excerpt.toString(),
          null,
          img,
          lastUpdatedDate,
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
    Set<String> membershipSet = getUserMemberships();
    if ((membershipSet != null) && (membershipSet.size()>0)) {
      String memberships = StringUtils.join(membershipSet.toArray(new String[membershipSet.size()]), "|");
      return "{\n" +
          "  \"term\" : { \"permissions\" : \"" + getCurrentUser() + "\" }\n" +
          "},\n" +
          "{\n" +
          "  \"regexp\" : { \"permissions\" : \"" + memberships + "\" }\n" +
          "}";
    }
    else {
      return "{\n" +
          "  \"term\" : { \"permissions\" : \"" + getCurrentUser() + "\" }\n" +
          "}";
    }
  }

  private String getSitesFilter(Collection<String> sitesCollection) {
    if ((sitesCollection != null) && (sitesCollection.size()>0)) {
      String sites = StringUtils.join(sitesCollection.toArray(new String[sitesCollection.size()]), "|");
      return "{\n" +
          "  \"not\": {\n" +
          "    \"exists\" : { \"field\" : \"sites\" }\n" +
          "  }\n" +
          "},\n" +
          "{\n" +
          "  \"regexp\" : { \n" +
          "    \"sites\" : \"" + sites + "\"\n" +
          "  }\n" +
          "}";
    }
    else {
      return "{\n" +
          "  \"not\": {\n" +
          "    \"exists\" : { \"field\" : \"sites\" }\n" +
          "  }\n" +
          "}";
    }
  }

  private String getCurrentUser() {
    ConversationState conversationState = ConversationState.getCurrent();
    if (conversationState == null) {
      throw new IllegalStateException("No Identity found: ConversationState.getCurrent() is null");
    }
    if (ConversationState.getCurrent().getIdentity()==null) {
      throw new IllegalStateException("No Identity found: ConversationState.getCurrent().getIdentity() is null");
    }
    return ConversationState.getCurrent().getIdentity().getUserId();
  }

  private Set<String> getUserMemberships() {
    ConversationState conversationState = ConversationState.getCurrent();
    if (conversationState == null) {
      throw new IllegalStateException("No Identity found: ConversationState.getCurrent() is null");
    }
    if (ConversationState.getCurrent().getIdentity()==null) {
      throw new IllegalStateException("No Identity found: ConversationState.getCurrent().getIdentity() is null");
    }
    if (ConversationState.getCurrent().getIdentity().getMemberships()==null) {
      //This case is not supported
      //The doc says "Any anonymous user automatically becomes a member of the group guests.group when they enter the public pages."
      //http://docs.exoplatform.com/PLF42/sect-Reference_Guide-Portal_Default_Permission_Configuration.html
      throw new IllegalStateException("No Membership found: ConversationState.getCurrent().getIdentity().getMemberships() is null");
    }

    Set<String> entries = new HashSet<>();
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

  private String getSourceFields() {

    List<String> fields = new ArrayList<>();
    fields.add("url");
    fields.add(getTitleElasticFieldName());

    List<String> sourceFields = new ArrayList<>();
    for (String sourceField: fields) {
      sourceFields.add("\"" + sourceField + "\"");
    }

    return StringUtils.join(sourceFields, ",");
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getImg() {
    return img;
  }

  public void setImg(String img) {
    this.img = img;
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

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}

