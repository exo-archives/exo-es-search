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

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.After;
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
    assertEquals(indexingOperations.size(), 0);
    IndexingOperation indexingOperation = new IndexingOperation();
    indexingOperation.setEntityType("blog");
    indexingOperation.setOperation(OperationType.INIT);

    //When
    indexingOperationDAO.create(indexingOperation);

    //Then
    assertEquals(indexingOperationDAO.findAll().size(), 1);
  }

  @Test
  public void testDatabaseAutoGeneratingTimestamp () {
    //Given
    IndexingOperation indexingOperation1 = new IndexingOperation();
    indexingOperation1.setEntityType("blog");
    indexingOperation1.setOperation(OperationType.INIT);
    indexingOperationDAO.create(indexingOperation1);
    IndexingOperation indexingOperation2 = new IndexingOperation();
    indexingOperation2.setEntityType("blog");
    indexingOperation2.setOperation(OperationType.INIT);
    indexingOperationDAO.create(indexingOperation2);
    //When
    indexingOperation1 = indexingOperationDAO.find(indexingOperation1.getId());
    indexingOperation2 = indexingOperationDAO.find(indexingOperation2.getId());
    //Then
    assertThat(indexingOperation1.getTimestamp(), lessThanOrEqualTo(indexingOperation2.getTimestamp()));
  }

}

