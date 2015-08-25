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
package org.exoplatform.addons.es.dao.impl;

import org.exoplatform.addons.es.dao.IndexingQueueDAO;
import org.exoplatform.addons.es.domain.IndexingQueue;
import org.exoplatform.commons.api.persistence.ExoTransactional;
import org.exoplatform.commons.persistence.impl.GenericDAOJPAImpl;

import java.util.Date;
import java.util.List;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/29/15
 */
public class IndexingQueueDAOImpl extends GenericDAOJPAImpl<IndexingQueue, Long> implements IndexingQueueDAO {

  @Override
  @ExoTransactional
  public List<IndexingQueue> findQueueFromLastTime(Date lastTime) {
    return getEntityManager()
            .createNamedQuery("IndexingQueue.findAllIndexingQueueFromLastTime", IndexingQueue.class)
            .setParameter("lastTime", lastTime)
            .getResultList();
  }

  @Override
  @ExoTransactional
  public List<IndexingQueue> findQueueBeforeLastTime(Date lastTime) {
    //TODO TEST
    return getEntityManager()
        .createNamedQuery("IndexingQueue.findAllIndexingQueueBeforeLastTime", IndexingQueue.class)
        .setParameter("lastTime", lastTime)
        .getResultList();
  }

  @Override
  @ExoTransactional
  public List<IndexingQueue> findQueueBeforeLastTimeByOperation(Date lastTime, String operation) {
    //TODO TEST
    return getEntityManager()
        .createNamedQuery("IndexingQueue.findAllIndexingQueueBeforeLastTimeByOperation", IndexingQueue.class)
        .setParameter("lastTime", lastTime)
        .setParameter("operation", operation)
        .getResultList();
  }

  @Override
  @ExoTransactional
  public List<IndexingQueue> findQueueBeforeLastTimeByOperations(Date lastTime, List<String> operations) {
    //TODO TEST
    return getEntityManager()
        .createNamedQuery("IndexingQueue.findAllIndexingQueueBeforeLastTimeByOperations", IndexingQueue.class)
        .setParameter("lastTime", lastTime)
        .setParameter("operations", operations)
        .getResultList();
  }

  @Override
  @ExoTransactional
  public Date getCurrentTimestamp() {
    //TODO return Timestamp from DB :)
    return new Date();
  }

  @Override
  @ExoTransactional
  public void DeleteAllBefore(Date lastTime) {
    //TODO create criteria to delete only row before lastTime instead deleteAll()
    deleteAll();
  }

}

