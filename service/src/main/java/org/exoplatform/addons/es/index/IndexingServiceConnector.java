package org.exoplatform.addons.es.index;

import org.exoplatform.addons.es.domain.Document;

/**
 * Created by TClement on 8/25/15.
 */
public interface IndexingServiceConnector {

  /**
   *
   * Initialise the create
   *
   * @return JSON containing create initialisation parameters
   *
   * @LevelAPI Experimental
   */
  public String init();

  /**
   *
   * Transform an entity to Document in order to be indexed
   *
   * @param id Id of entity to create
   *
   * @return List of Document to create
   *
   * @LevelAPI Experimental
   */
  public Document create(String id);

  /**
   *
   * Transform an entity to Document in order to be reindexed
   *
   * @param id Id of entity to reindex
   *
   * @return List of Document to reindex
   *
   * @LevelAPI Experimental
   */
  public Document update (String id);

  /**
   *
   * Transform a list of entities to Document in order to be deleted from create
   *
   * @param id Ids of entities to delete from
   *
   * @return List of Ids to delete from create
   *
   * @LevelAPI Experimental
   */
  public abstract String delete (String id);

}
