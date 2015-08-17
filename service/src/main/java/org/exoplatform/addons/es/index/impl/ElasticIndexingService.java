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
package org.exoplatform.addons.es.index.impl;

import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.addons.es.index.IndexingServiceConnector;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/29/15
 */
public class ElasticIndexingService extends IndexingService {

  @Override
  public void addToIndexQueue(IndexingServiceConnector indexingServiceConnector, Long id, String operation) {

    // TODO 1: Every time a searchable data is modified in PLF, its entityID+entityType is inserted in an
    // “indexing queue” table on the RDBMS, indicating that this entity needs to be reindexed (or deleted from
    // the index in case of deletion).
    //The row is inserted in the indexing queue with the operation type (C-reated, U-pdated, D-eleted). That way,
    // in case of deletion, there is no need to query PLF to obtain the entity: a deletion operation is just sent to ES.

    //TODO 2: If a row already exists for this entity in the table, it is updated (using “ON DUPLICATE KEY UPDATE”
    // SQL statement)
  }

  @Override
  public void index() {

    // TODO 1: Process all the requests for “init of the ES index mapping” (Operation type = I) in the indexing queue
    // (if any)

    // TODO 2: Process all the requests for “remove all documents of type” (Operation type = X) in the indexing queue
    // (if any)

    // TODO 3: Process the indexing requests (per batchs of 1000 rows max. This value is configurable) In detail:
    // get the timestamp of start of processing
    // get the list of indexing requests from the indexing queue that are older than this timestamp
    // if the operation type is C-reated or U-pdated, obtain the last version of the modified entity from PLF and
    // generate a Json index request
    // if the operation type is D-eleted, generate a Json delete request
    // computes a bulk update request with all the unitary requests
    // submits this request to ES
    // then removes the processed IDs from the “indexing queue” table that have timestamp older than the timestamp of
    // start of processing

    // TODO 4: In case of error, the entityID+entityType will be logged in a “error queue” to allow a manual
    // reprocessing of the indexing operation. However, in a first version of the implementation, the error will only
    // be logged with ERROR level in the log file of platform.
  }
}

