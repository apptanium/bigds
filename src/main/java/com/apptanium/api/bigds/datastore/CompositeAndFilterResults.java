package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * does not support sorting; currently follows lexicographically ascending sort order
 * this will be updated as the code evolves to include sorting on ONE dimension
 *
 * @author sgupta
 * @since 1/21/15.
 */
public class CompositeAndFilterResults extends QueryResults {

  private final List<Scan> scans;
  private final Table indexTable;
  private final ResultScanner firstScanner;
  private final Iterator<Result> firstResultIterator;

  CompositeAndFilterResults(DatastoreService datastoreService,
                            Query query,
                            List<Scan> scans,
                            Table indexTable) throws IOException {
    super(datastoreService, query);
    this.scans = scans;
    this.indexTable = indexTable;
    this.firstScanner = indexTable.getScanner(this.scans.get(0));
    this.firstResultIterator = this.firstScanner.iterator();
  }

  @Override
  public boolean hasNext() {
    return this.firstResultIterator.hasNext();
  }

  @Override
  public Entity next() {
    Result result = firstResultIterator.next();
    byte[] parentBytes = result.getValue(DatastoreConstants.ID_COLUMN_FAMILY_BYTES, DatastoreConstants.ID_COLUMN_PARENT_BYTES);
    byte[] keyIdBytes = result.getValue(DatastoreConstants.ID_COLUMN_FAMILY_BYTES, DatastoreConstants.ID_COLUMN_KEY_BYTES);

    return null;
  }

  @Override
  public void close() {

  }

  //todo: implement getCursor() for composite 'AND' filter results
  @Override
  public Cursor getCursor() {
    return null;
  }
}
