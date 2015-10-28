package org.exoplatform.addons.es.client;

import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

/**
 * Created by The eXo Platform SAS Author : eXoPlatform exo@exoplatform.com
 * 10/28/15
 */
public class ElasticIndexingAuditTrail {
  private static final Log AUDIT_TRAIL = ExoLogger.getExoLogger("org.exoplatform.indexing.es");

  void audit(String action, String entityId, String index, String type, int httpStatusCode, String message, long executionTime) {
    AUDIT_TRAIL.info("{};{};{};{};{};{};{}", action, entityId, index, type, httpStatusCode, message, executionTime);
  }

  void logRejectedDocument(String action,
                           String entityId,
                           String index,
                           String type,
                           int httpStatusCode,
                           String message,
                           long executionTime) {
    AUDIT_TRAIL.error("{};{};{};{};{};{};{}", action, entityId, index, type, httpStatusCode, message, executionTime);
  }
}
