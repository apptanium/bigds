package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;
import com.apptanium.api.bigds.entity.Key;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

/**
 * @author sgupta
 * @since 1/21/15.
 */
public class CompositeOrFilterResults extends QueryResults {

  private final Connection connection;
  private final TableName indexTableName;
  private final Scan[] scans;
  private final ResultScanner[] resultScanners;
  private final boolean[] available;
  private final List<Iterator<Result>> resultIterators;
  private final Set<Key> keys = new HashSet<>();
  private int loop = 0;

  CompositeOrFilterResults(DatastoreService datastoreService,
                           Query query,
                           Scan[] scans,
                           Connection connection,
                           TableName indexTableName) throws IOException {
    super(datastoreService, query);
    this.scans = scans;
    this.resultScanners = new ResultScanner[scans.length];
    this.available = new boolean[this.resultScanners.length];
    Arrays.fill(this.available, true);
    this.resultIterators = new ArrayList<>(this.resultScanners.length);
    for (int i = 0; i < scans.length; i++) {
      Table indexTable = connection.getTable(indexTableName);
      Scan scan = scans[i];
      resultScanners[i] = indexTable.getScanner(scan);
      resultIterators.add(resultScanners[i].iterator());
    }
    this.connection = connection;
    this.indexTableName = indexTableName;
  }

  /**
   *
   * @return true if there is at least one dimension that can still be iterated over
   */
  @Override
  public boolean hasNext() {
    boolean hasNext = false;
    for (Iterator<Result> resultIterator : resultIterators) {
      hasNext |= resultIterator.hasNext();
    }
    return hasNext;
  }

  @Override
  public Entity next() {
    Entity next = null;
    int missedIterators = 0;
    do {
      Iterator<Result> iterator = resultIterators.get(loop % resultIterators.size());
      if(iterator.hasNext()) {
        Result result = iterator.next();
        Key key = DatastoreUtils.createKeyFromIndexRow(result, query.getKind());
        if(!keys.contains(key)) {
          keys.add(key);
          next = datastoreService.get(key);
        }
      }
      else {
        missedIterators++;
      }
      loop++;
    } while (next == null && missedIterators < resultIterators.size());
    return next;
  }

  /**
   * always remember to close the results!
   */
  @Override
  public void close() {
    for (ResultScanner resultScanner : resultScanners) {
      resultScanner.close();
    }
  }

  //todo: implement getCursor() for composite 'OR' filter results
  @Override
  public Cursor getCursor() {
    return null;
  }
}
