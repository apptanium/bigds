package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;
import com.apptanium.api.bigds.entity.Key;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public interface DatastoreService {

  public Key put(Entity entity);

  public List<Key> put(Iterable<Entity> entities);

//  public List<Key> put(Entity... entities);

  public Map<Key,Entity> get(Iterable<Key> keys);

  public Entity get(Key key);

  public void delete(Iterable<Key> keys);

  public void delete(Key... keys);

  public List<Index> getIndexes();

}
