package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
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

  private final List<List<Scan>> dnfScans;
  private final List<Scan> rangeScans;
  private final Table indexTable;
  private final List<ResultScanner> resultScanners = new ArrayList<>();
  private final List<Iterator<Result>> resultIterators = new ArrayList<>();

  CompositeAndFilterResults(DatastoreService datastoreService,
                            Query query,
                            List<List<Scan>> dnfScans,
                            List<Scan> rangeScans,
                            Table indexTable) throws IOException {
    super(datastoreService, query);
    this.dnfScans = dnfScans;
    this.rangeScans = rangeScans;
    this.indexTable = indexTable;
    if (rangeScans.size() > 0) {
      for (Scan rangeScan : rangeScans) {
        ResultScanner scanner = this.indexTable.getScanner(rangeScan);
        resultScanners.add(scanner);
        resultIterators.add(scanner.iterator());
      }
    }
    else {

    }
    //todo: build set of iterators that control the top iteration cycle
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = true;
    for (Iterator<Result> resultIterator : resultIterators) {
      hasNext &= resultIterator.hasNext();
    }
    return hasNext;
  }

  @Override
  public Entity next() {
    //todo: implement simultaneous iterations across all relevant iterators
    //todo: implement tests for scans that are not part of the top level iterators
/*
    Result result = firstResultIterator.next();
    byte[] parentBytes = result.getValue(DatastoreConstants.ID_COLUMN_FAMILY_BYTES, DatastoreConstants.ID_COLUMN_PARENT_BYTES);
    byte[] keyIdBytes = result.getValue(DatastoreConstants.ID_COLUMN_FAMILY_BYTES, DatastoreConstants.ID_COLUMN_KEY_BYTES);
*/

    return null;
  }

  @Override
  public void close() {
    for (ResultScanner resultScanner : resultScanners) {
      resultScanner.close();
    }
    //todo: also close all the dnf scans
  }

  //todo: implement getCursor() for composite 'AND' filter results
  @Override
  public Cursor getCursor() {
    return null;
  }
}
