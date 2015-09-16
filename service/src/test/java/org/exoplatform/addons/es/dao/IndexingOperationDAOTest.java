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
package org.exoplatform.addons.es.dao;

import java.util.List;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.exoplatform.addons.es.domain.IndexingOperation;
import org.exoplatform.addons.es.domain.OperationType;
import org.exoplatform.container.PortalContainer;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/20/15
 */
public class IndexingOperationDAOTest extends AbstractDAOTest {

  private IndexingOperationDAO indexingOperationDAO;

  @Before
  public void setUp() {
    PortalContainer container = PortalContainer.getInstance();
    indexingOperationDAO = container.getComponentInstanceOfType(IndexingOperationDAO.class);
  }

  @After
  public void tearDown() {
    indexingOperationDAO.deleteAll();
  }

  @Test
  public void testIndexingQueueCreation() {

    //Given
    List<IndexingOperation> indexingOperations = indexingOperationDAO.findAll();
    Assert.assertEquals(indexingOperations.size(), 0);
    IndexingOperation indexingOperation = new IndexingOperation();
    indexingOperation.setEntityType("blog");
    indexingOperation.setOperation(OperationType.INIT);

    //When
    indexingOperationDAO.create(indexingOperation);

    //Then
    Assert.assertEquals(indexingOperationDAO.findAll().size(), 1);
  }

  @Test
  public void testDatabaseAutoGeneratingTimestamp () {

    //Given
    Long startDate = System.currentTimeMillis();
    IndexingOperation indexingOperation = new IndexingOperation();
    indexingOperation.setEntityType("blog");
    indexingOperation.setOperation(OperationType.INIT);

    //When
    indexingOperationDAO.create(indexingOperation);

    //Then
    indexingOperation = indexingOperationDAO.find(indexingOperation.getId());
    Assert.assertThat(
        startDate,
        Matchers.lessThanOrEqualTo(indexingOperation.getTimestamp().getTime()));
    Assert.assertThat(
        System.currentTimeMillis(),
        Matchers.greaterThanOrEqualTo(indexingOperation.getTimestamp().getTime()));
  }

}

