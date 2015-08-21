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
package org.exoplatform.addons.es.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Date;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/22/15
 */
@Entity
@Table(name = "ES_INDEX_QUEUE")
@NamedQueries({
    @NamedQuery(name = "IndexingQueue.findAllIndexingQueueFromLastTime",
        query = "SELECT q FROM IndexingQueue q WHERE q.timestamp >= :lastTime"),
    @NamedQuery(name = "IndexingQueue.findAllIndexingQueueBeforeLastTime",
        query = "SELECT q FROM IndexingQueue q WHERE q.timestamp <= :lastTime GROUP BY q.operation"),
    @NamedQuery(name = "IndexingQueue.findAllIndexingQueueBeforeLastTimeByOperation",
        query = "SELECT q FROM IndexingQueue q WHERE q.timestamp <= :lastTime AND q.operation = :operation"),
    @NamedQuery(name = "IndexingQueue.findAllIndexingQueueBeforeLastTimeByOperations",
        query = "SELECT q FROM IndexingQueue q WHERE q.timestamp <= :lastTime AND q.operation IN :operations")
})
public class IndexingQueue {

  @Id
  @GeneratedValue
  @Column(name = "QUEUE_ID")
  Long id;

  @Column(name = "ENTITY_TYPE")
  String entityType;

  @Column(name = "ENTITY_ID")
  String entityId;

  @Column(name = "OPERATION_TYPE")
  String operation;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "OPERATION_TIMESTAMP", insertable = false, updatable = false)
  Date timestamp;

  public IndexingQueue() {
  }

  public IndexingQueue(Long id, String entityId, String entityType, String operation, Date timestamp) {
    this.id = id;
    this.entityId = entityId;
    this.entityType = entityType;
    this.operation = operation;
    this.timestamp = timestamp;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getEntityId() {
    return entityId;
  }

  public void setEntityId(String entityId) {
    this.entityId = entityId;
  }

  public String getEntityType() {
    return entityType;
  }

  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public Date getTimestamp() {
    return timestamp;
  }

}

