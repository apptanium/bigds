package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.EntityUtils;
import com.apptanium.api.bigds.entity.ValueConverter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author sgupta
 * @since 1/19/15.
 */
public class CompiledQuery implements DatastoreConstants {

  private final DatastoreService datastoreService;
  private final String namespace;
  private final Connection connection;
  private final Query query;
  private final TableName tableName;
  private final TableName indexesTableName;

  CompiledQuery(DatastoreService datastoreService, String namespace, Connection connection, Query query) {
    this.datastoreService = datastoreService;
    this.namespace = namespace;
    this.connection = connection;
    this.query = query;
    this.tableName = TableName.valueOf(namespace, query.getKind());
    this.indexesTableName = TableName.valueOf(namespace, "_" + query.getKind());
  }

  public QueryResults getResults() throws IOException {
    QueryFilter filter = query.getFilter();
    if(filter instanceof CompositeFilter) {
      CompositeFilter compositeFilter = (CompositeFilter) filter;
      QueryFilter.CompositeOperator operator = compositeFilter.getOperator();
      switch (operator) {
        case AND:

          break;

        case OR:
          break;
      }
    }
    else if(filter instanceof PropertyFilter) {
      return getResultsForPropertyFilter((PropertyFilter) filter);
      //is a simple property filter
    }
    return null;
  }

  private QueryResults getResultsForPropertyFilter(PropertyFilter filter) throws IOException {
    if(!EntityUtils.isApprovedClass(filter.getValue().getClass())) {
      throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "class " + filter.getValue().getClass().getName() + " is not supported in the data store");
    }
    ValueConverter valueConverter = null;
    if(filter.getOperator()== QueryFilter.Operator.IN) {
      List list = (List) filter.getValue();
      valueConverter = EntityUtils.getValueConverter(list.get(0).getClass());
    }
    else {
      valueConverter = EntityUtils.getValueConverter(filter.getValue().getClass());
    }
    if(!valueConverter.isIndexable()) {
      throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "class " + filter.getValue().getClass().getName() + " is not indexable");
    }
    String propertyName = filter.getPropertyName();
    Table indexTable = connection.getTable(indexesTableName);
    Scan scan = new Scan();
    scan.addFamily(ID_COLUMN_FAMILY_BYTES);

    switch (filter.getOperator()) {

      case EQUAL:
        scan.setRowPrefixFilter(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()));
        break;

      case GREATER_THAN:
        scan.setStartRow(Bytes.padTail(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()), 1));
        break;

      case GREATER_THAN_OR_EQUAL:
        scan.setStartRow(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()));
        break;

      case IN:
/*
        FilterList inFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        List list = (List) filter.getValue();
        byte[][] prefixes = new byte[list.size()][];
        for (int i = 0; i < list.size(); i++) {
          Object val = list.get(i);
          prefixes[i] = valueConverter.convertToRowPrefixId(propertyName, val);
          inFilterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new ByteArrayComparable() {
            @Override
            public byte[] toByteArray() {
              return new byte[0];
            }

            @Override
            public int compareTo(byte[] value, int offset, int length) {
              return 0;
            }
          }));
        }
        scan.setFilter(inFilterList);
        break;
*/
        throw new RuntimeException("in operator not implemented yet");

      case LESS_THAN:
        scan.setStopRow(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()));
        break;

      case LESS_THAN_OR_EQUAL:
        scan.setStopRow(Bytes.padTail(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()), 1));
        break;

      case NOT_EQUAL:
/*
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()));
        columnPrefixFilter.set
*/
        throw new RuntimeException("not equal operator not implemented yet");
    }

    if(query.getLimit() > 0 && query.getLimit() < 1000) {
      scan.setMaxResultSize(query.getLimit());
    }
    else {
      scan.setMaxResultSize(100);
    }
    if(query.getOffset() > 0) {
      scan.setRowOffsetPerColumnFamily(query.getOffset());
    }
    ResultScanner resultScanner = indexTable.getScanner(scan);
    return new QueryResults(datastoreService, query, resultScanner);
  }
}
