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
package org.exoplatform.addons.es.rest;

import org.exoplatform.addons.es.domain.IndexingOperation;
import org.exoplatform.addons.es.index.IndexingOperationProcessor;
import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.IndexingServiceConnector;
import org.exoplatform.addons.es.index.impl.QueueIndexingService;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.services.rest.resource.ResourceContainer;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 10/6/15
 */
@Path("/indexingManagement")
@Produces(MediaType.APPLICATION_JSON)
@RolesAllowed("administrators")
public class IndexingManagementRestService implements ResourceContainer {

  private final static Log LOG = ExoLogger.getLogger(IndexingManagementRestService.class);

  private QueueIndexingService indexingService;
  private IndexingOperationProcessor indexingOperationProcessor;

  public IndexingManagementRestService(IndexingService indexingService, IndexingOperationProcessor indexingOperationProcessor) {
    this.indexingService = (QueueIndexingService) indexingService;
    this.indexingOperationProcessor = indexingOperationProcessor;
  }

  // Indexing Connector

  @GET
  @Path("/connector")
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed("administrators")
  public Response getConnectors() {
    List<IndexingServiceConnector> connectors = new ArrayList<>(indexingOperationProcessor.getConnectors().values());
    return Response.ok(connectors, MediaType.APPLICATION_JSON).build();
  }

  @GET
  @Path("/connector/{connectorType}/_reindex")
  @RolesAllowed("administrators")
  public Response reindexConnector(@PathParam("connectorType") String connectorType) {
    indexingService.reindexAll(connectorType);
    return Response.ok("ok", MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/connector/{connectorType}/_disable")
  @RolesAllowed("administrators")
  public Response disable(@PathParam("connectorType") String connectorType) {
    //TODO implement a disable connector method in IndexingService
    return Response.ok("ok", MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/connector/{connectorType}/_enable")
  @RolesAllowed("administrators")
  public Response enable(@PathParam("connectorType") String connectorType) {
    //TODO implement a enable connector method in IndexingService
    return Response.ok("ok", MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/connector/_count")
  @RolesAllowed("administrators")
  public Response getNbConnectors() {
    return Response.ok(String.valueOf(indexingOperationProcessor.getConnectors().size()), MediaType.TEXT_PLAIN).build();
  }

  // Indexing Operation

  @GET
  @Path("/operation")
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed("administrators")
  public Response getOperations() throws ParseException {
    List<IndexingOperation> operations = new ArrayList<>(indexingService.getOperations(0,100));
    //List<IndexingOperation> operations = getFakeIndexingOperation();
    return Response.ok(operations, MediaType.APPLICATION_JSON).build();
  }

  @GET
  @Path("/operation/_count")
  @RolesAllowed("administrators")
  public Response getNbIndexingOperations() {
    return Response.ok(String.valueOf(indexingService.getNumberOperations()), MediaType.TEXT_PLAIN).build();
  }

  // Indexing Error

  @GET
  @Path("/error")
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed("administrators")
  public Response getErrors() {
    //TODO
    return Response.ok(null, MediaType.APPLICATION_JSON).build();
  }

  @GET
  @Path("/error/_count")
  @RolesAllowed("administrators")
  public Response getNbIndexingErrors() {
    //TODO
    return Response.ok("0", MediaType.TEXT_PLAIN).build();
  }
}
