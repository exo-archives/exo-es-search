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

import org.exoplatform.addons.es.client.ElasticContentRequestBuilder;
import org.exoplatform.addons.es.client.ElasticIndexingClient;
import org.exoplatform.addons.es.dao.IndexingOperationDAO;
import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.addons.es.domain.IndexingOperation;
import org.exoplatform.addons.es.domain.OperationType;
import org.exoplatform.addons.es.index.impl.ElasticIndexingOperationProcessor;
import org.exoplatform.addons.es.index.impl.ElasticIndexingServiceConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 9/1/15
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticOperationProcessorTest {

  //Naming Convention Used: methodUnderTest_conditionEncounter_resultExpected

  private ElasticIndexingOperationProcessor elasticIndexingOperationProcessor;

  @Mock
  private IndexingOperationDAO indexingOperationDAO;

  @Mock
  private ElasticIndexingClient elasticIndexingClient;

  @Mock
  private ElasticIndexingServiceConnector elasticIndexingServiceConnector;

  @Mock
  private ElasticContentRequestBuilder elasticContentRequestBuilder;

  @Captor
  private ArgumentCaptor<String> stringCaptor;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
    elasticIndexingOperationProcessor = new ElasticIndexingOperationProcessor(indexingOperationDAO, elasticIndexingClient, elasticContentRequestBuilder);
    initElasticServiceConnector();
  }

  private void initElasticServiceConnector() {
    when(elasticIndexingServiceConnector.getIndex()).thenReturn("blog");
    when(elasticIndexingServiceConnector.getType()).thenReturn("post");
    when(elasticIndexingServiceConnector.getReplicas()).thenReturn(1);
    when(elasticIndexingServiceConnector.getShards()).thenReturn(5);
  }

  @After
  public void clean() {
    elasticIndexingOperationProcessor.getConnectors().clear();
  }

  /*
  addConnector(ElasticIndexingServiceConnector elasticIndexingServiceConnector)
   */

  @Test
  public void addConnector_ifNewConnector_connectorAdded() {
    //Given
    assertEquals(0, elasticIndexingOperationProcessor.getConnectors().size());
    //When
    elasticIndexingOperationProcessor.addConnector(elasticIndexingServiceConnector);
    //Then
    assertEquals(1, elasticIndexingOperationProcessor.getConnectors().size());
  }

  @Test
  public void addConnector_ifConnectorAlreadyExist_connectorNotAdded() {
    //Given
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    assertEquals(1, elasticIndexingOperationProcessor.getConnectors().size());
    //When
    elasticIndexingOperationProcessor.addConnector(elasticIndexingServiceConnector);
    //Then
    assertEquals(1, elasticIndexingOperationProcessor.getConnectors().size());
  }

  @Test
  public void addConnectRor_ifNewConnector_initIndexingQueueCreated() {
    //Given
    IndexingOperation indexingOperation = new IndexingOperation(null,null,"post", OperationType.INIT);
    //When
    elasticIndexingOperationProcessor.addConnector(elasticIndexingServiceConnector);
    //Then
    verify(indexingOperationDAO, times(1)).create(indexingOperation);
  }

  @Test
  public void addConnector_ifConnectorAlreadyExist_initIndexingQueueNotCreated() {
    //Given
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation indexingOperation = new IndexingOperation(null,null,"post",OperationType.INIT);
    //When
    elasticIndexingOperationProcessor.addConnector(elasticIndexingServiceConnector);
    //Then
    verify(indexingOperationDAO, times(0)).create(indexingOperation);
  }

  /*
  process()
   */

  //test the order of the operation processing


  @Test
  public void process_ifAllOperationsInQueue_requestShouldBeSentInAnExpectedOrder() throws ParseException {
    //Given
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation create = new IndexingOperation(3l,"1","post",OperationType.CREATE);
    IndexingOperation delete = new IndexingOperation(4l,"1","post",OperationType.DELETE);
    IndexingOperation update = new IndexingOperation(5l,"1","post",OperationType.UPDATE);
    IndexingOperation deleteAll = new IndexingOperation(2l,null,"post",OperationType.DELETE_ALL);
    IndexingOperation init = new IndexingOperation(1l,null,"post",OperationType.INIT);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create);
    indexingOperations.add(delete);
    indexingOperations.add(update);
    indexingOperations.add(init);
    indexingOperations.add(deleteAll);
    Document document = new Document("post", "1", null, new Date(), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.create("1")).thenReturn(document);
    when(elasticIndexingServiceConnector.update("1")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then

    //Check Client invocation
    InOrder orderClient = inOrder(elasticIndexingClient);
    //Operation I
    orderClient.verify(elasticIndexingClient).sendCreateIndexRequest(elasticIndexingServiceConnector.getIndex(),
        elasticContentRequestBuilder.getCreateIndexRequestContent(elasticIndexingServiceConnector));
    orderClient.verify(elasticIndexingClient).sendCreateTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType(),
        elasticIndexingServiceConnector.getMapping());
    //Then Operation X
    orderClient.verify(elasticIndexingClient).sendDeleteTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType());
    orderClient.verify(elasticIndexingClient).sendCreateTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType(),
        elasticIndexingServiceConnector.getMapping());
    //Then Operation D, C and U
    orderClient.verify(elasticIndexingClient).sendCUDRequest(anyString());
    //Then no more interaction with client
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifAllOperationsInQueue_requestShouldBeCreatedInAnExpectedOrder() throws ParseException {
    //Given
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    elasticIndexingOperationProcessor.getConnectors().put("post1", elasticIndexingServiceConnector);
    elasticIndexingOperationProcessor.getConnectors().put("post2", elasticIndexingServiceConnector);
    elasticIndexingOperationProcessor.getConnectors().put("post3", elasticIndexingServiceConnector);
    IndexingOperation init = new IndexingOperation(4l,null,"post",OperationType.INIT);
    IndexingOperation deleteAll = new IndexingOperation(5l,null,"post",OperationType.DELETE_ALL);
    IndexingOperation delete = new IndexingOperation(2l,"1","post1",OperationType.DELETE);
    IndexingOperation create = new IndexingOperation(1l,"2","post2",OperationType.CREATE);
    IndexingOperation update = new IndexingOperation(3l,"3","post3",OperationType.UPDATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create);
    indexingOperations.add(delete);
    indexingOperations.add(update);
    indexingOperations.add(init);
    indexingOperations.add(deleteAll);
    Document document = new Document("post", "1", null, new Date(), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.create("2")).thenReturn(document);
    when(elasticIndexingServiceConnector.update("3")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then

    //Check Request Builder invocation
    InOrder orderRequestBuilder = inOrder(elasticContentRequestBuilder);
    //Operation I
    orderRequestBuilder.verify(elasticContentRequestBuilder).getCreateIndexRequestContent(elasticIndexingServiceConnector);
    //Then Operation D, C and U
    orderRequestBuilder.verify(elasticContentRequestBuilder).getDeleteDocumentRequestContent(any(ElasticIndexingServiceConnector.class), anyString());
    orderRequestBuilder.verify(elasticContentRequestBuilder).getCreateDocumentRequestContent(elasticIndexingServiceConnector, "2");
    orderRequestBuilder.verify(elasticContentRequestBuilder).getUpdateDocumentRequestContent(elasticIndexingServiceConnector, "3");
    //Then no more interaction with builder
    verifyNoMoreInteractions(elasticContentRequestBuilder);
  }

  //test the result of operation processing on the operation still in queue

  @Test
  public void process_ifDeleteAllOperation_allOldestCreateUpdateDeleteOperationsWithSameTypeStillInQueueShouldBeCanceled() throws ParseException {
    //Given
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation deleteAll = new IndexingOperation(5l,null,"post",OperationType.DELETE_ALL);
    //CUD operation are older than delete all
    IndexingOperation create = new IndexingOperation(1l,"1","post",OperationType.CREATE);
    IndexingOperation delete = new IndexingOperation(2l,"1","post",OperationType.DELETE);
    IndexingOperation update = new IndexingOperation(3l,"1","post",OperationType.UPDATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create);
    indexingOperations.add(delete);
    indexingOperations.add(update);
    indexingOperations.add(deleteAll);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    InOrder orderClient = inOrder(elasticIndexingClient);
    //Remove and recreate type request
    orderClient.verify(elasticIndexingClient).sendDeleteTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType());
    orderClient.verify(elasticIndexingClient).sendCreateTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType(),
        elasticIndexingServiceConnector.getMapping());
    //No CUD request
    verifyNoMoreInteractions(elasticIndexingClient);
  }


  @Test
  public void process_ifDeleteOperation_allOldestCreateOperationsWithSameEntityIdStillInQueueShouldBeCanceled() throws ParseException {
    //Given
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    //Delete operation are older than create and update
    IndexingOperation delete = new IndexingOperation(2l,"1","post",OperationType.DELETE);
    IndexingOperation create = new IndexingOperation(1l,"1","post",OperationType.CREATE);
    IndexingOperation update = new IndexingOperation(3l,"1","post",OperationType.UPDATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create);
    indexingOperations.add(delete);
    indexingOperations.add(update);
    Document document = new Document("post", "1", null, new Date(), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.create("1")).thenReturn(document);
    when(elasticIndexingServiceConnector.update("1")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    InOrder order = inOrder(elasticIndexingClient);
    //Only one delete request should be build
    verify(elasticContentRequestBuilder, times(1)).getDeleteDocumentRequestContent(elasticIndexingServiceConnector, "1");
    verifyNoMoreInteractions(elasticContentRequestBuilder);
    //Only one CUD request should be send
    order.verify(elasticIndexingClient, times(1)).sendCUDRequest(anyString());
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifCreateOperation_allOldestAndNewestUpdateOperationsWithSameEntityIdStillInQueueShouldBeCanceled() throws ParseException {
    //Given
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation create = new IndexingOperation(1l,"1","post",OperationType.CREATE);
    IndexingOperation oldUpdate = new IndexingOperation(3l,"1","post",OperationType.UPDATE);
    IndexingOperation newUpdate = new IndexingOperation(3l,"1","post",OperationType.UPDATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create);
    indexingOperations.add(oldUpdate);
    indexingOperations.add(newUpdate);
    Document document = new Document("post", "1", null, sdf.parse("19/01/1989"), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.create("1")).thenReturn(document);
    when(elasticIndexingServiceConnector.update("1")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    InOrder order = inOrder(elasticIndexingClient);
    //Only one create request should be build (update operations are canceled)
    verify(elasticContentRequestBuilder, times(1)).getCreateDocumentRequestContent(elasticIndexingServiceConnector, "1");
    verifyNoMoreInteractions(elasticContentRequestBuilder);
    //Only one CUD request should be send
    order.verify(elasticIndexingClient, times(1)).sendCUDRequest(anyString());
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifDeleteAllOperation_allNewestCreateDeleteOperationsStillInQueueShouldBeProcessed() throws ParseException {
    //Given
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation deleteAll = new IndexingOperation(1l,null,"post",OperationType.DELETE_ALL);
    //CUD operation are newer than delete all
    IndexingOperation create = new IndexingOperation(3l,"1","post",OperationType.CREATE);
    IndexingOperation delete = new IndexingOperation(2l,"1","post",OperationType.DELETE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create);
    indexingOperations.add(delete);
    indexingOperations.add(deleteAll);
    Document document = new Document("post", "1", null, sdf.parse("19/01/1989"), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.create("1")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    InOrder orderClient = inOrder(elasticIndexingClient);
    //Remove and recreate type request
    orderClient.verify(elasticIndexingClient).sendDeleteTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType());
    orderClient.verify(elasticIndexingClient).sendCreateTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType(),
        elasticIndexingServiceConnector.getMapping());
    //Create and Delete requests should be build
    verify(elasticContentRequestBuilder, times(1)).getCreateDocumentRequestContent(elasticIndexingServiceConnector, "1");
    verify(elasticContentRequestBuilder, times(1)).getDeleteDocumentRequestContent(elasticIndexingServiceConnector, "1");
    //Only one CUD request should be send
    orderClient.verify(elasticIndexingClient, times(1)).sendCUDRequest(anyString());
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifDeleteOperation_allNewestCreateOperationsStillInQueueShouldBeProcessed() throws ParseException {
    //Given
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    //Delete operation are older than create
    IndexingOperation delete = new IndexingOperation(1l,"1","post",OperationType.DELETE);
    IndexingOperation create = new IndexingOperation(2l,"1","post",OperationType.CREATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create);
    indexingOperations.add(delete);
    Document document = new Document("post", "1", null, sdf.parse("19/01/1989"), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.create("1")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    InOrder order = inOrder(elasticContentRequestBuilder);
    //Only one delete and one create request should be build
    order.verify(elasticContentRequestBuilder, times(1)).getDeleteDocumentRequestContent(elasticIndexingServiceConnector, "1");
    order.verify(elasticContentRequestBuilder, times(1)).getCreateDocumentRequestContent(elasticIndexingServiceConnector, "1");
    verifyNoMoreInteractions(elasticContentRequestBuilder);
    //Only one CUD request should be send
    verify(elasticIndexingClient, times(1)).sendCUDRequest(anyString());
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifDeleteAllOperation_allNewestUpdateOperationsWithSameEntityTypeIdStillInQueueShouldBeCanceled() throws ParseException {
    //Given
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation deleteAll = new IndexingOperation(5l,null,"post",OperationType.DELETE_ALL);
    //CUD operation are newer than delete all
    IndexingOperation update = new IndexingOperation(1l,"1","post",OperationType.UPDATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(update);
    indexingOperations.add(deleteAll);
    Document document = new Document("post", "1", null, sdf.parse("19/01/1989"), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.update("1")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    InOrder orderClient = inOrder(elasticIndexingClient);
    //Remove and recreate type request
    orderClient.verify(elasticIndexingClient).sendDeleteTypeRequest(elasticIndexingServiceConnector.getIndex(),
        elasticIndexingServiceConnector.getType());
    orderClient.verify(elasticIndexingClient).sendCreateTypeRequest(elasticIndexingServiceConnector.getIndex(),
            elasticIndexingServiceConnector.getType(),
            elasticIndexingServiceConnector.getMapping());
    //No CUD operation
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifDeleteOperation_allNewestUpdateOperationsWithSameEntityIdStillInQueueShouldBeCanceled() throws ParseException {
    //Given
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    //Delete operation are older than update
    IndexingOperation delete = new IndexingOperation(2l,"1","post",OperationType.DELETE);
    IndexingOperation update = new IndexingOperation(1l,"1","post",OperationType.UPDATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(update);
    indexingOperations.add(delete);
    Document document = new Document("post", "1", null, sdf.parse("19/01/1989"), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.update("1")).thenReturn(document);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    //Only one delete request should be build
    verify(elasticContentRequestBuilder, times(1)).getDeleteDocumentRequestContent(elasticIndexingServiceConnector, "1");
    verifyNoMoreInteractions(elasticContentRequestBuilder);
    //Only one CUD request should be send
    verify(elasticIndexingClient, times(1)).sendCUDRequest(anyString());
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifBulkRequestReachedSizeLimit_requestIsSent() throws ParseException {
    //Given
    elasticIndexingOperationProcessor.setRequestSizeLimit(1);
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation create1 = new IndexingOperation(1l,"1","post",OperationType.CREATE);
    IndexingOperation create2 = new IndexingOperation(2l,"2","post",OperationType.CREATE);
    List<IndexingOperation> indexingOperations = new ArrayList<>();
    indexingOperations.add(create1);
    indexingOperations.add(create2);
    Document document1 = new Document("post", "1", null, sdf.parse("19/01/1989"), null, null);
    Document document2 = new Document("post", "2", null, sdf.parse("19/01/1989"), null, null);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(indexingOperations);
    when(elasticIndexingServiceConnector.create("1")).thenReturn(document1);
    when(elasticIndexingServiceConnector.create("2")).thenReturn(document2);

    //When
    elasticIndexingOperationProcessor.process();

    //Then
    //Two CUD request should be send because the first create request will reached the limit size (= 1 byte)
    verify(elasticIndexingClient, times(2)).sendCUDRequest(anyString());
    verifyNoMoreInteractions(elasticIndexingClient);
  }

  @Test
  public void process_ifReindexAll_requestIsSent() throws ParseException {
    //Given
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    elasticIndexingOperationProcessor.getConnectors().put("post", elasticIndexingServiceConnector);
    IndexingOperation reindexAll = new IndexingOperation(5l,null,"post",OperationType.REINDEX_ALL);
    when(indexingOperationDAO.findAllFirst(anyInt())).thenReturn(Arrays.asList(reindexAll));
    when(elasticIndexingServiceConnector.getAllIds()).thenReturn(Arrays.asList("1","2"));
    //When
    elasticIndexingOperationProcessor.process();
    //Then
    InOrder orderClient = inOrder(indexingOperationDAO);
    orderClient.verify(indexingOperationDAO).create(new IndexingOperation(null, null,
            elasticIndexingServiceConnector.getType(), OperationType.DELETE_ALL));
    orderClient.verify(indexingOperationDAO).create(new IndexingOperation(null, "1",
            elasticIndexingServiceConnector.getType(), OperationType.UPDATE));
    orderClient.verify(indexingOperationDAO).create(new IndexingOperation(null, "2",
            elasticIndexingServiceConnector.getType(), OperationType.UPDATE));
    verifyNoMoreInteractions(elasticIndexingClient);
  }

}

