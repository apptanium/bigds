package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.EntityUtils;
import com.apptanium.api.bigds.entity.ValueConverter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.*;

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
          return getResultsForAndCompositeFilter(compositeFilter);

        case OR:
          return getResultsForOrCompositeFilter(compositeFilter);
      }
    }
    else if(filter instanceof PropertyFilter) {
      return getResultsForPropertyFilter((PropertyFilter) filter);
      //is a simple property filter
    }
    return null;
  }

  private QueryResults getResultsForAndCompositeFilter(CompositeFilter compositeFilter) throws IOException {
    List<PropertyFilter> filters = compositeFilter.getFilters();
    if (filters.size() == 0) {
      throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "AND composite filter must have at least 2 property filters");
    }
    if (filters.size() == 1) {
      return getResultsForPropertyFilter(filters.get(0));
    }
    String rangeProperty = null;
    List<Scan> rangeScans = new ArrayList<>();
    List<List<Scan>> inListScans = new ArrayList<>();
    List<Scan> propertyScans = new ArrayList<>();
    Table indexTable = connection.getTable(indexesTableName);
    //todo: complete logic for AND filtering
    int ltCount = 0;
    int gtCount = 0;
    int neCount = 0;
    PropertyFilter gtFilter, ltFilter;
    Scan gtScan = null, ltScan = null;
    List<List<Scan>> dnfScans = null;
    for (PropertyFilter filter : filters) {
      Scan[] scanArray = getScanFromPropertyFilter(filter);
      switch (filter.getOperator()) {
        case IN:
          inListScans.add(Arrays.asList(scanArray));
          break;

        case EQUAL:
          propertyScans.add(scanArray[0]);
          break;

        default: {
          if (rangeProperty == null) {
            rangeProperty = filter.getPropertyName();
          }
          else {
            if (!rangeProperty.equals(filter.getPropertyName())) {
              throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "Only one range property is allowed");
            }
          }

          if (filter.getOperator() == QueryFilter.Operator.GREATER_THAN ||
              filter.getOperator() == QueryFilter.Operator.GREATER_THAN_OR_EQUAL) {
            gtFilter = filter;
            gtCount++;
            gtScan = scanArray[0];
          }
          else if (filter.getOperator() == QueryFilter.Operator.LESS_THAN ||
                   filter.getOperator() == QueryFilter.Operator.LESS_THAN_OR_EQUAL) {
            ltFilter = filter;
            ltCount++;
            ltScan = scanArray[0];
          }
          else if (filter.getOperator() == QueryFilter.Operator.NOT_EQUAL) {
            neCount++;
          }

          //Validate counts
          if (gtCount > 1 || ltCount > 1 || neCount > 1) {
            throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "Range directions cannot be duplicated");
          }

          //validate mix of ne and gt/lt scans
          if (neCount > 0 && (gtCount > 0 || ltCount > 0)) {
            throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "NOT_EQUALS cannot be mixed with GT or LT operators");
          }
          rangeScans.addAll(Arrays.asList(scanArray));
          break;
        }
      }
      if (gtScan != null && ltScan != null) {
        Scan collapsedScan = new Scan();
        collapsedScan.addFamily(ID_COLUMN_FAMILY_BYTES);
        collapsedScan.setStartRow(gtScan.getStartRow());
        collapsedScan.setStopRow(ltScan.getStopRow());
        rangeScans.clear(); //remove old scans
        rangeScans.add(collapsedScan); //put only the collapsed scan as a range scan
      }


      dnfScans = new ArrayList<>();
      buildInListMatrix(inListScans, new Stack<Scan>(), 0, dnfScans);
      if (dnfScans.size() == 0) {
        dnfScans.add(propertyScans);
      }
      else {
        for (List<Scan> dnfScan : dnfScans) {
          dnfScan.addAll(propertyScans);
        }
      }


    }
    return new CompositeAndFilterResults(datastoreService, query, dnfScans, rangeScans, indexTable);
  }

  private void buildInListMatrix(List<List<Scan>> inListScans, Stack<Scan> stack, int index, List<List<Scan>> matrix) {
    if(index == inListScans.size()) {
      return;
    }
    List<Scan> list = inListScans.get(index);
    for (Scan scan : list) {
      if(index + 1 < inListScans.size()) {
        stack.push(scan);
        buildInListMatrix(inListScans, stack, index + 1, matrix);
        stack.pop();
      }
      else {
        List<Scan> row = new ArrayList<>();
        row.addAll(stack);
        row.add(scan);
        matrix.add(row);
      }
    }
  }


  private QueryResults getResultsForOrCompositeFilter(CompositeFilter compositeFilter) throws IOException {
    List<PropertyFilter> filters = compositeFilter.getFilters();
    if(filters.size() == 0) {
      throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "AND composite filter must have at least 2 property filters");
    }
    if(filters.size() == 1) {
      return getResultsForPropertyFilter(filters.get(0));
    }
    else {
      List<Scan> listOfScans = new ArrayList<>();
      for (PropertyFilter filter : filters) {
        Scan[] scans = getScanFromPropertyFilter(filter);
        Collections.addAll(listOfScans, scans);
      }
      return new CompositeOrFilterResults(datastoreService,
                                          query,
                                          listOfScans.toArray(new Scan[listOfScans.size()]),
                                          connection,
                                          indexesTableName);
    }
  }

  private QueryResults getResultsForPropertyFilter(PropertyFilter filter) throws IOException {
    Table indexTable = null;
    try {
      Scan[] scans = getScanFromPropertyFilter(filter);

      if(scans.length == 1) {
        Scan scan = scans[0];
        if (query.getLimit() > 0 && query.getLimit() < 1000) {
          scan.setMaxResultSize(query.getLimit());
        }
        else {
          scan.setMaxResultSize(100);
        }
        if (query.getOffset() > 0) {
          scan.setRowOffsetPerColumnFamily(query.getOffset());
        }
        indexTable = connection.getTable(indexesTableName);
        ResultScanner resultScanner = indexTable.getScanner(scan);
        return new PropertyFilterResults(datastoreService, query, resultScanner);
      }
      else {
        return new CompositeOrFilterResults(datastoreService, query, scans, connection, indexesTableName);
      }

    }
    finally {
      if (indexTable != null) {
        indexTable.close();
      }
    }
  }


  private Scan[] getScanFromPropertyFilter(PropertyFilter filter) throws IOException {
    if(filter.getOperator() == QueryFilter.Operator.IN) {
      if(!List.class.isAssignableFrom(filter.getValue().getClass())) {
        throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "'IN' operator requires a list of supported values as filter operand");
      }
    }
    else if(!EntityUtils.isApprovedClass(filter.getValue().getClass())) {
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
    Scan scan = new Scan();
    scan.addFamily(ID_COLUMN_FAMILY_BYTES);

    switch (filter.getOperator()) {

      case EQUAL:
        scan.setRowPrefixFilter(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()));
        break;

      case GREATER_THAN: {
        byte[] startValue = valueConverter.convertToRowPrefixId(propertyName, filter.getValue());
        startValue[startValue.length - 1] = (byte)((int)startValue[startValue.length-1] + 1);
        scan.setStartRow(startValue);
        scan.setStopRow((propertyName + ">").getBytes(CHARSET));
        break;
      }

      case GREATER_THAN_OR_EQUAL:
        //start row is inclusive by default
        scan.setStartRow(valueConverter.convertToRowPrefixId(propertyName, filter.getValue()));
        scan.setStopRow((propertyName + ">").getBytes(CHARSET)); // '>' is the byte after '='
        break;

      case IN: {
        List valueList = (List) filter.getValue();
        Scan[] scanList = new Scan[valueList.size()];
        int i = 0;
        for (Object value : valueList) {
          Scan currScan = new Scan();
          currScan.addFamily(ID_COLUMN_FAMILY_BYTES);
          currScan.setRowPrefixFilter(valueConverter.convertToRowPrefixId(propertyName, value));
          scanList[i] = currScan;
          i++;
        }
        return scanList;
      }

      case LESS_THAN:
        //stop row is exclusive by default
        scan.setStartRow((propertyName + "=").getBytes(CHARSET)); // start is inclusive, so start from propertyName=
        scan.setStopRow(valueConverter.convertToRowPrefixId(propertyName, filter.getValue())); //stop is exclusive by default
        break;

      case LESS_THAN_OR_EQUAL: {
        scan.setStartRow((propertyName + "=").getBytes(CHARSET));
        byte[] stopValue = valueConverter.convertToRowPrefixId(propertyName, filter.getValue());
        stopValue[stopValue.length - 1] = (byte) ((int) stopValue[stopValue.length - 1] +1);
        scan.setStopRow(stopValue); //stop is exclusive, so stop one byte after the max
        break;
      }

      case NOT_EQUAL: {
        Scan[] scanList = new Scan[2];
        //exclusively less than
        scanList[0] = new Scan(scan);
        scanList[0].setStartRow((propertyName + "=").getBytes(CHARSET)); //inclusive
        scanList[0].setStopRow(valueConverter.convertToRowPrefixId(propertyName, filter.getValue())); //exclusive

        //exclusively greater than
        scanList[1] = new Scan(scan);
        byte[] upperBoundStart = valueConverter.convertToRowPrefixId(propertyName, filter.getValue());
        upperBoundStart[upperBoundStart.length - 1] = (byte)((int)upperBoundStart[upperBoundStart.length-1] + 1);
        scanList[1].setStartRow(upperBoundStart);
        scanList[1].setStopRow((propertyName + ">").getBytes(CHARSET));
        return scanList;
      }

    }
    return new Scan[]{scan};

  }
}
