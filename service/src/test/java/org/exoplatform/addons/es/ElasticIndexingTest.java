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

import org.exoplatform.addons.es.blogpost.BlogPostElasticIndexingServiceConnector;
import org.exoplatform.addons.es.blogpost.Blogpost;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingService;
import org.exoplatform.addons.es.index.elastic.ElasticIndexingServiceConnector;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.PropertiesParam;
import org.junit.Test;

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
public class ElasticIndexingTest extends AbstractElasticTest {

  @Test
  public void testAddConnector() {

    //Given
    Map<String, ElasticIndexingServiceConnector> indexingServiceConnectors = indexingService.getConnectors();
    assertEquals(0, indexingServiceConnectors.size());
    addBlogPostConnector();

    //When
    indexingServiceConnectors = indexingService.getConnectors();

    //Then
    assertEquals(1, indexingServiceConnectors.size());
  }

  @Test
  public void testCreateNewIndex() {

    //Given
    assertFalse(indexExists(CONNECTOR_INDEX));
    addBlogPostConnector();

    //When
    indexingService.process();

    //Then
    assertTrue(indexExists(CONNECTOR_INDEX));

  }

  @Test
  public void testCreateNewType() {

    //Given
    assertFalse(typeExists(CONNECTOR_TYPE));
    addBlogPostConnector();

    //When
    indexingService.process();

    //Then
    assertTrue(typeExists(CONNECTOR_TYPE));

  }

  @Test
  public void testIndexingDocument() throws InterruptedException {

    //Given
    addBlogPostConnector();
    //assertEquals(0, typeDocumentNumber(CONNECTOR_TYPE));
    blogpostService.initData();
    for (Blogpost blogpost: blogpostService.findAll()) {
      indexingService.addToIndexQueue(CONNECTOR_TYPE, String.valueOf(blogpost.getId()), ElasticIndexingService.CREATE);
    }

    //When
    indexingService.process();
    Thread.sleep(2 * 1000);

    //Then
    assertEquals(blogpostService.findAll().size(), typeDocumentNumber(CONNECTOR_TYPE));

  }

  @Test
  public void testDeleteAllIndexedDocument() throws InterruptedException {

    //Given
    addBlogPostConnector();
    blogpostService.initData();
    for (Blogpost blogpost: blogpostService.findAll()) {
      indexingService.addToIndexQueue(CONNECTOR_TYPE, String.valueOf(blogpost.getId()), ElasticIndexingService.CREATE);
    }
    indexingService.process();
    Thread.sleep(2 * 1000);
    assertTrue(typeExists(CONNECTOR_TYPE));
    assertEquals(blogpostService.findAll().size(), typeDocumentNumber(CONNECTOR_TYPE));

    //When
    indexingService.addToIndexQueue(CONNECTOR_TYPE, null, ElasticIndexingService.DELETE_ALL);
    indexingService.process();

    //Then
    //Type is existing but it has no document
    assertTrue(typeExists(CONNECTOR_TYPE));
    assertEquals(0, typeDocumentNumber(CONNECTOR_TYPE));
  }

  private void addBlogPostConnector() {
    PropertiesParam propertiesParam = new PropertiesParam();
    propertiesParam.getProperties().put("index", CONNECTOR_INDEX);
    propertiesParam.getProperties().put("type", CONNECTOR_TYPE);
    InitParams initParams = new InitParams();
    initParams.put("constructor.params", propertiesParam);

    indexingService.addConnector(new BlogPostElasticIndexingServiceConnector(initParams, blogpostService));
  }



}

