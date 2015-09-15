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

import java.util.Date;

import javax.persistence.*;

import org.exoplatform.commons.api.persistence.ExoEntity;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/22/15
 */
@Entity
@ExoEntity
@Table(name = "ES_INDEXING_QUEUE")
@NamedQueries({
    @NamedQuery(name = "IndexingOperation.findAllIndexingOperationsFromLastTime",
        query = "SELECT q FROM IndexingOperation q WHERE q.timestamp >= :lastTime"),
    @NamedQuery(name = "IndexingOperation.findAllIndexingOperationsBeforeLastTime",
        query = "SELECT q FROM IndexingOperation q WHERE q.timestamp <= :lastTime GROUP BY q.operation"),
    @NamedQuery(name = "IndexingOperation.findAllIndexingOperationsBeforeLastTimeByOperation",
        query = "SELECT q FROM IndexingOperation q WHERE q.timestamp <= :lastTime AND q.operation = :operation"),
    @NamedQuery(name = "IndexingOperation.findAllIndexingOperationsBeforeLastTimeByOperations",
        query = "SELECT q FROM IndexingOperation q WHERE q.timestamp <= :lastTime AND q.operation IN :operations"),
    @NamedQuery(name = "IndexingOperation.deleteBeforeTimestamp",
        query = "DELETE FROM IndexingOperation q WHERE q.timestamp <= :lastTime")
})
public class IndexingOperation {

  @Id
  @GeneratedValue
  @Column(name = "OPERATION_ID")
  private Long id;

  @Column(name = "ENTITY_TYPE")
  private String entityType;

  @Column(name = "ENTITY_ID")
  private String entityId;

  @Column(name = "OPERATION_TYPE")
  private String operation;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "OPERATION_TIMESTAMP", insertable = false, updatable = false)
  private Date timestamp;

  public IndexingOperation() {
  }

  public IndexingOperation(Long id, String entityId, String entityType, OperationType operation, Date timestamp) {
    this.id = id;
    this.entityId = entityId;
    this.entityType = entityType;
    this.setOperation(operation);
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

  public OperationType getOperation() {
    return this.operation==null?null:OperationType.getById(this.operation);
  }

  public void setOperation(OperationType operation) {
    this.operation = operation==null?null:operation.getOperationId();
  }

  public Date getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IndexingOperation that = (IndexingOperation) o;

    if (entityId != null ? !entityId.equals(that.entityId) : that.entityId != null) return false;
    if (entityType != null ? !entityType.equals(that.entityType) : that.entityType != null) return false;
    if (id != null ? !id.equals(that.id) : that.id != null) return false;
    if (operation != null ? !operation.equals(that.operation) : that.operation != null) return false;
    if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (entityType != null ? entityType.hashCode() : 0);
    result = 31 * result + (entityId != null ? entityId.hashCode() : 0);
    result = 31 * result + (operation != null ? operation.hashCode() : 0);
    result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
    return result;
  }
}

