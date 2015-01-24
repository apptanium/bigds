package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;
import com.apptanium.api.bigds.entity.Key;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.Iterator;

/**
 * @author sgupta
 * @since 1/21/15.
 */
public class PropertyFilterResults extends QueryResults {
  private final ResultScanner resultScanner;
  private final Iterator<Result> resultIterator;

  PropertyFilterResults(DatastoreService datastoreService,
                        Query query,
                        ResultScanner resultScanner) {
    super(datastoreService, query);
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
    Key key = DatastoreUtils.createKeyFromIndexRow(result, query.getKind());
    return datastoreService.get(key);
  }

  /**
   * always remember to close the results!
   */
  @Override
  public void close() {
    resultScanner.close();
  }

  //todo: implement getCursor() for property filter results
  @Override
  public Cursor getCursor() {
    return null;
  }
}
