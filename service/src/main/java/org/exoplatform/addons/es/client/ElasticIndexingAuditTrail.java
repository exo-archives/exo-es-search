package org.exoplatform.addons.es.client;

import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

/**
 * Created by The eXo Platform SAS Author : eXoPlatform exo@exoplatform.com
 * 10/28/15
 */
public class ElasticIndexingAuditTrail {
  private static final Log AUDIT_TRAIL = ExoLogger.getExoLogger("org.exoplatform.indexing.es");
  public static final String REINDEX_ALL = "reindex_all";

  public void audit(String action, String entityId, String index, String type, Integer httpStatusCode, String message, long executionTime) {
    AUDIT_TRAIL.info("{};{};{};{};{};{};{}", action, entityId, index, type, httpStatusCode, message, executionTime);
  }

  public void logRejectedDocument(String action,
                           String entityId,
                           String index,
                           String type,
                           int httpStatusCode,
                           String message,
                           long executionTime) {
    AUDIT_TRAIL.error("{};{};{};{};{};{};{}", action, entityId, index, type, httpStatusCode, message, executionTime);
  }
}
