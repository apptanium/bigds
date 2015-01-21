package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;
import com.apptanium.api.bigds.entity.Key;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.Iterator;

/**
 * @author sgupta
 * @since 1/19/15.
 */
public class QueryResults implements Iterator<Entity> {
  private static final byte slash = '/';
  private final DatastoreService datastoreService;
  private final Query query;
  private final ResultScanner resultScanner;
  private final Iterator<Result> resultIterator;

  QueryResults(DatastoreService datastoreService, Query query, ResultScanner resultScanner) {
    this.datastoreService = datastoreService;
    this.query = query;
    this.resultScanner = resultScanner;
    this.resultIterator = this.resultScanner.iterator();
  }


  @Override
  public boolean hasNext() {
    return this.resultIterator.hasNext();
  }

  @Override
  public Entity next() {
    Result result = resultIterator.next();
    byte[] parentBytes = result.getValue(DatastoreConstants.ID_COLUMN_FAMILY_BYTES, DatastoreConstants.ID_COLUMN_KEY_BYTES);
    byte[] rowId = result.getRow();
    int index = rowId.length-1;
    for (; index >= 0; index--) {
      if(rowId[index]==slash){
        break;
      }
    }
    Key parent = parentBytes != null && parentBytes.length > 1 ? Key.createKey(new String(parentBytes, DatastoreConstants.CHARSET), false) : null;
    Key key = new Key(parent, query.getKind(), new String(rowId, index+1, rowId.length - index -1));
    Entity entity = datastoreService.get(key);
    return entity;
  }

  /**
   * always remember to close the results!
   */
  public void close() {
    resultScanner.close();
  }
}
