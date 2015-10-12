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

import org.exoplatform.addons.es.dao.IndexingOperationDAO;
import org.exoplatform.addons.es.domain.IndexingOperation;
import org.exoplatform.addons.es.domain.OperationType;
import org.exoplatform.addons.es.index.impl.QueueIndexingService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.text.ParseException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 10/12/15
 */
@RunWith(MockitoJUnitRunner.class)
public class QueueIndexingServiceTest {

  //Naming Convention Used: methodUnderTest_conditionEncounter_resultExpected

  private QueueIndexingService queueIndexingService;

  @Mock
  private IndexingOperationDAO indexingOperationDAO;

  @Captor
  private ArgumentCaptor<String> stringCaptor;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
    queueIndexingService = new QueueIndexingService(indexingOperationDAO);
  }

  @After
  public void clean() {
    queueIndexingService.clearIndexingQueue();
  }

  /*
  indexing Method
   */

  @Test
  public void init_ifInitOperation_initIndexingQueueCreated() {
    //Given
    IndexingOperation indexingOperation = new IndexingOperation(null,null,"post",OperationType.INIT,null);
    //When
    queueIndexingService.init("post");
    //Then
    verify(indexingOperationDAO, times(1)).create(indexingOperation);
  }

  @Test
  public void unindexAll_ifDeleteAllOperation_deleteAllIndexingQueueCreated() {
    //Given
    IndexingOperation indexingOperation = new IndexingOperation(null,null,"post",OperationType.DELETE_ALL,null);
    //When
    queueIndexingService.unindexAll("post");
    //Then
    verify(indexingOperationDAO, times(1)).create(indexingOperation);
  }

  @Test
  public void unindex_ifDeleteOperation_deleteIndexingQueueCreated() {
    //Given
    IndexingOperation indexingOperation = new IndexingOperation(null,"1","post",OperationType.DELETE,null);
    //When
    queueIndexingService.unindex("post", "1");
    //Then
    verify(indexingOperationDAO, times(1)).create(indexingOperation);
  }

  @Test
  public void reindex_ifUpdateOperation_updateIndexingQueueCreated() {
    //Given
    IndexingOperation indexingOperation = new IndexingOperation(null,"1","post",OperationType.UPDATE,null);
    //When
    queueIndexingService.reindex("post", "1");
    //Then
    verify(indexingOperationDAO, times(1)).create(indexingOperation);
  }

  @Test
  public void reindexAll_commandsAreInsertedInIndexingQueue() throws ParseException {
    //Given
    IndexingOperation indexingOperation = new IndexingOperation(null,null,"post",OperationType.REINDEX_ALL,null);
    //When
    queueIndexingService.reindexAll("post");
    //Then
    verify(indexingOperationDAO, times(1)).create(indexingOperation);
  }

  @Test
  public void index_ifCreateOperation_createIndexingQueueCreated() {
    //Given
    IndexingOperation indexingOperation = new IndexingOperation(null,"1","post",OperationType.CREATE,null);
    //When
    queueIndexingService.index("post", "1");
    //Then
    verify(indexingOperationDAO, times(1)).create(indexingOperation);
  }

  @Test
  public void addToIndexQueue_ifNoEntityId_CUDIndexingQueueNotCreated() {
    //TODO not implemented yet
    //Given
    //When
    //Then
  }

  @Test
  public void addToIndexQueue_ifNoConnectorExist_triggerException() {
    //TODO implement specific exception
    //Given
    //When
    //queueIndexingService.addToIndexingQueue("post", null, ElasticIndexingService.CREATE);
    //Then
  }

}

