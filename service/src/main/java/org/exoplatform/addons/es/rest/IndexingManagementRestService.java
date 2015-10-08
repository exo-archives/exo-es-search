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

import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.IndexingServiceConnector;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.services.rest.resource.ResourceContainer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
public class IndexingManagementRestService implements ResourceContainer {

  private final static Log LOG = ExoLogger.getLogger(IndexingManagementRestService.class);

  private IndexingService indexingService;

  public IndexingManagementRestService(IndexingService indexingService) {
    this.indexingService = indexingService;
  }

  @GET
  @Path("/connector")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConnectors() {
    LOG.info("Call getConnectors via REST");
    List<IndexingServiceConnector> connectors = new ArrayList<>(indexingService.getConnectors().values());
    return Response.ok(connectors, MediaType.APPLICATION_JSON).build();
  }

  @GET
  @Path("/connector/{connectorType}/_reindex")
  public Response reindexConnector(@PathParam("connectorType") String connectorType) {
    LOG.info("Call reindexConnector via REST");
    indexingService.reindexAll(connectorType);
    return Response.ok("ok", MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/connector/{connectorType}/_disable")
  public Response disable(@PathParam("connectorType") String connectorType) {
    LOG.info("Call disable via REST");
    //TODO implement a disable connector method in IndexingService
    return Response.ok("ok", MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/connector/{connectorType}/_enable")
  public Response enable(@PathParam("connectorType") String connectorType) {
    LOG.info("Call disable via REST");
    //TODO implement a enable connector method in IndexingService
    return Response.ok("ok", MediaType.TEXT_PLAIN).build();
  }

}

