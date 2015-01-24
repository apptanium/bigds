package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;

import java.util.Iterator;

/**
 * @author sgupta
 * @since 1/19/15.
 */
public abstract class QueryResults implements Iterator<Entity> {
  protected final DatastoreService datastoreService;
  protected final Query query;

  protected QueryResults(DatastoreService datastoreService, Query query) {
    this.datastoreService = datastoreService;
    this.query = query;
  }

  public abstract void close();
  public abstract Cursor getCursor();

}
