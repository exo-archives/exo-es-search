package org.exoplatform.addons.es.client;

import org.apache.commons.lang.StringUtils;

import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

/**
 * Created by The eXo Platform SAS Author : eXoPlatform exo@exoplatform.com
 * 10/28/15
 */
public class ElasticIndexingAuditTrail {
  public static final String REINDEX_ALL = "reindex_all";

  public static final String DELETE_ALL  = "delete_all";

  private static final Log   AUDIT_TRAIL = ExoLogger.getExoLogger("org.exoplatform.indexing.es");

  public void audit(String action,
                    String entityId,
                    String index,
                    String type,
                    Integer httpStatusCode,
                    String message,
                    long executionTime) {
    AUDIT_TRAIL.info("{};{};{};{};{};{};{}",
                     action,
                     StringUtils.isBlank(entityId) ? "" : entityId,
                     StringUtils.isBlank(index) ? "" : index,
                     StringUtils.isBlank(type) ? "" : type,
                     httpStatusCode == null ? "" : httpStatusCode,
                     StringUtils.isBlank(message) ? "" : message,
                     executionTime);
  }

  public void logRejectedDocument(String action,
                                  String entityId,
                                  String index,
                                  String type,
                                  int httpStatusCode,
                                  String message,
                                  long executionTime) {
    AUDIT_TRAIL.error("{};{};{};{};{};{};{}",
                      action,
                      StringUtils.isBlank(entityId) ? "" : entityId,
                      StringUtils.isBlank(index) ? "" : index,
                      StringUtils.isBlank(type) ? "" : type,
                      httpStatusCode,
                      StringUtils.isBlank(message) ? "" : message,
                      executionTime);
  }
}
