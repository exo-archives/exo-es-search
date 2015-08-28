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

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.addons.es.blogpost.BlogPostElasticIndexingServiceConnector;
import org.exoplatform.addons.es.blogpost.Blogpost;
import org.exoplatform.addons.es.blogpost.BlogpostElasticSearchConnector;
import org.exoplatform.addons.es.blogpost.BlogpostService;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingService;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingServiceConnector;
import org.exoplatform.addons.es.search.ElasticSearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.container.PortalContainer;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

/*

WARNING:
THIS TEST CAN WORK ONLY WHEN YOU START A LOCAL ES ON YOUR MACHINE FIRST.
IT"S  ALL TESTS ARE COMMENTED. IF YOU WANT TO TEST IT START LOCAL ES AND UNCOMMENT THE TEST.
TO FIX IT WE NEED TO BE ABLE TO START AN EMBEDDED ES

 */

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/20/15
 */
public class ElasticTest extends AbstractElasticTest {

  private static final String CONNECTOR_INDEX = "blog";
  private static final String CONNECTOR_TYPE = "post";

  private IndexingService indexingService;
  private BlogpostService blogpostService;
  private ElasticSearchServiceConnector searchServiceConnector;

  @Before
  public void setUp() {
    PortalContainer container = PortalContainer.getInstance();
    indexingService = container.getComponentInstanceOfType(IndexingService.class);
    blogpostService = new BlogpostService();
    searchServiceConnector = getBlogPostSearchConnector();
  }

  @After
  public void tearDown() {
    indexingService.getConnectors().clear();
    indexingService.clearIndexQueue();
    cleanElastic();
  }

  //@Test
  public void testInitConnector() {
    //Given
    Map<String, ElasticIndexingServiceConnector> indexingServiceConnectors = indexingService.getConnectors();
    Assert.assertEquals(0, indexingServiceConnectors.size());
    addBlogPostConnector();

    //When
    indexingServiceConnectors = indexingService.getConnectors();

    //Then
    Assert.assertEquals(1, indexingServiceConnectors.size());
  }

  //@Test
  public void testCreateIndex() {
    //Given
    addBlogPostConnector();

    //When
    indexingService.process();

    //Then
    //TODO Check if create well created and type also. First we need to start an embedded ES
    //Thibault: I check locally with my ES and it's working
  }

  //@Test
  public void testDeleteAllIndexedDocument() {

    //Given
    addBlogPostConnector();
    indexingService.addToIndexQueue(CONNECTOR_TYPE, null, ElasticIndexingService.DELETE_ALL);

    //When
    indexingService.process();

    //Then
    //TODO Check if a bulk delete request has been send
    //Thibault: I check locally with my ES and it's working
  }

  //@Test
  public void testIndexingDocument() {

    //Given
    addBlogPostConnector();
    blogpostService.initData();
    for (Blogpost blogpost: blogpostService.findAll()) {
      indexingService.addToIndexQueue(CONNECTOR_TYPE, String.valueOf(blogpost.getId()), ElasticIndexingService.CREATE);
    }

    //When
    indexingService.process();

    //Then
    //TODO Check if a CUD bulk request has been send and that all document are indexed
    //Thibault: I check locally with my ES and it's working

  }

  //@Test
  public void testSearchingDocument() throws InterruptedException {

    //Given
    addBlogPostConnector();
    blogpostService.initData();
    for (Blogpost blogpost: blogpostService.findAll()) {
      indexingService.addToIndexQueue(CONNECTOR_TYPE, String.valueOf(blogpost.getId()), ElasticIndexingService.CREATE);
    }
    indexingService.process();
    Thread.sleep(2 * 1000);


    //When
    Collection<SearchResult> searchResults =
        searchServiceConnector.search(null, "death", null, 0, 10, "relevancy", "asc");

    //Then
    Assert.assertEquals(1, searchResults.size());
    //Thibault: I check locally with my ES and it's working

  }

  private void addBlogPostConnector() {
    PropertiesParam propertiesParam = new PropertiesParam();
    propertiesParam.getProperties().put("index", CONNECTOR_INDEX);
    propertiesParam.getProperties().put("type", CONNECTOR_TYPE);
    InitParams initParams = new InitParams();
    initParams.put("constructor.params", propertiesParam);

    indexingService.addConnector(new BlogPostElasticIndexingServiceConnector(initParams, blogpostService));
  }

  private BlogpostElasticSearchConnector getBlogPostSearchConnector() {

    PropertiesParam propertiesParam = new PropertiesParam();
    propertiesParam.getProperties().put("index", CONNECTOR_INDEX);
    propertiesParam.getProperties().put("type", CONNECTOR_TYPE);
    propertiesParam.getProperties().put("fields", "title,content,author");
    InitParams initParams = new InitParams();
    initParams.put("constructor.params", propertiesParam);

    return new BlogpostElasticSearchConnector(initParams);
  }

  private void cleanElastic() {
    HttpClient client = new DefaultHttpClient();

    HttpDelete httpDeleteRequest = new HttpDelete("http://127.0.0.1:9200/" + CONNECTOR_INDEX);
  }

  private Blogpost getDefaultBlogpost() {

    Blogpost deathStartProject = new Blogpost();
    deathStartProject.setId(1L);
    deathStartProject.setAuthor("Thibault");
    deathStartProject.setPostDate(new Date());
    deathStartProject.setTitle("Death Star Project");
    deathStartProject.setContent("The new Death Star Project is under the responsability of the Vador Team : Benoit," +
        " Thibault and Tuyen");
    return deathStartProject;

  }

}

