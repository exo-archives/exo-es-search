package org.exoplatform.addons.es.index;

import java.util.List;

import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.container.component.BaseComponentPlugin;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 7/22/15
 */
public abstract class IndexingServiceConnector extends BaseComponentPlugin {

  private String type;

  /**
   * Transform an entity to Document in order to be indexed
   * @param id Id of entity to create
   * @return List of Document to create
   * @LevelAPI Experimental
   */
  public abstract Document create(String id);

  /**
   * Transform an entity to Document in order to be reindexed
   * @param id Id of entity to reindex
   * @return List of Document to reindex
   * @LevelAPI Experimental
   */
  public abstract Document update(String id);

  /**
   * Transform a list of entities to Document in order to be deleted from create
   * @param id Ids of entities to delete from
   * @return List of Ids to delete from create
   * @LevelAPI Experimental
   */
  public abstract String delete (String id);

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public abstract List<String> getAllIds(int offset, int limit);
}
