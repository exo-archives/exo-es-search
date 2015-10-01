/*
 * Copyright (C) 2015 eXo Platform SAS.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.exoplatform.addons.es.job;

import org.exoplatform.addons.es.index.IndexingService;
import org.exoplatform.job.MultiTenancyJob;
import org.exoplatform.services.jcr.RepositoryService;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.quartz.JobExecutionContext;

/**
 * @author <a href="mailto:tuyennt@exoplatform.com">Tuyen Nguyen The</a>.
 */
public class BulkIndexingJob extends MultiTenancyJob {
  private static final Log LOG = ExoLogger.getExoLogger(BulkIndexingJob.class);

  @Override
  public Class<? extends MultiTenancyTask> getTask() {
    return IndexingBulkTask.class;
  }

  public class IndexingBulkTask extends MultiTenancyTask {
    public IndexingBulkTask(JobExecutionContext context, String repoName) {
      super(context, repoName);
    }

    @Override
    public void run() {
      super.run();
      LOG.debug("Running job BulkIndexingJob");

      // TODO: need to implement the cloud-supporting for JPA (EntityManagerService)

      RepositoryService repoService = container.getComponentInstanceOfType(RepositoryService.class);
      IndexingService indexingService = container.getComponentInstanceOfType(IndexingService.class);

      String defaultRepoName = repoService.getConfig().getDefaultRepositoryName();
      if (!repoName.equals(defaultRepoName)) {
        // This is in cloud environment: tenant name and repoName are the same
        IndexingService.setCurrentTenantName(repoName);
      }
      indexingService.process();
    }
  }
}
