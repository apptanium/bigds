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

  private final class InequalityScanStatus {
    private final Scan[] ltScan;
    private final Scan[] gtScan;
    private final Scan[] neScans;
    private final boolean isError;

    private InequalityScanStatus(Scan[] ltScan, Scan[] gtScan, Scan[] neScans, boolean isError) {
      this.ltScan = ltScan;
      this.gtScan = gtScan;
      this.neScans = neScans;
      this.isError = isError;
    }
  }

  private InequalityScanStatus getInequalityScans(List<PropertyFilter> filters) {
    int ltCount = 0; String ltField = null; Scan[] ltScan = null;
    int gtCount = 0; String gtField = null; Scan[] gtScan = null;
    int neCount = 0; String neField = null; Scan[] neScans = null;

    for (PropertyFilter filter : filters) {
      switch (filter.getOperator()) {
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
          gtCount++;
          if(gtField == null) {
            gtField = filter.getPropertyName();
          }
          try {
            gtScan = getScanFromPropertyFilter(filter);
          }
          catch (IOException e) {
            throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, e);
          }
          break;

        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          ltCount++;
          if(ltField == null) {
            ltField = filter.getPropertyName();
          }
          try {
            ltScan = getScanFromPropertyFilter(filter);
          }
          catch (IOException e) {
            throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, e);
          }
          break;

        case NOT_EQUAL:
          neCount++;
          neField = filter.getPropertyName();
          try {
            neScans = getScanFromPropertyFilter(filter);
          }
          catch (IOException e) {
            throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, e);
          }
          break;
      }
    }

    if(ltCount > 1 || gtCount > 1 || neCount > 1) {
      return new InequalityScanStatus(ltScan, gtScan, null, true);
    }

    if(neCount == 1) {
      if(gtCount > 0 || ltCount > 0) {
        return new InequalityScanStatus(ltScan, gtScan, null, true);
      }
      return new InequalityScanStatus(ltScan, gtScan, neScans, false);
    }

    if(ltCount == 1 && gtCount == 1) {
      if(ltField.equals(gtField)) {
        return new InequalityScanStatus(ltScan, gtScan, null, false);
      }
      return new InequalityScanStatus(null, null, null, true);
    }

    if(ltCount == 1) {
      return new InequalityScanStatus(ltScan, null, null, false);
    }

    if(gtCount == 1) {
      return new InequalityScanStatus(null, gtScan, gtScan, false);
    }

    //no error, and no inequality scans
    return new InequalityScanStatus(null, null, null, false);
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

    InequalityScanStatus iss = getInequalityScans(filters);
    if(!iss.isError) {
      if(iss.neScans != null) {
        rangeScans.add(iss.neScans[0]);
        rangeScans.add(iss.neScans[1]);
      }
      else if (iss.gtScan != null && iss.ltScan != null) {
        Scan collapsedScan = new Scan();
        collapsedScan.addFamily(ID_COLUMN_FAMILY_BYTES);
        collapsedScan.setStartRow(iss.gtScan[0].getStartRow());
        collapsedScan.setStopRow(iss.ltScan[0].getStopRow());
        rangeScans.add(collapsedScan); //put only the collapsed scan as a range scan
      }
      else if(iss.gtScan != null) {
        rangeScans.add(iss.gtScan[0]);
      }
      else if(iss.ltScan != null) {
        rangeScans.add(iss.ltScan[0]);
      }
    }

    List<List<Scan>> dnfScans = new ArrayList<>();

    for (PropertyFilter filter : filters) {
      Scan[] scanArray = getScanFromPropertyFilter(filter);
      switch (filter.getOperator()) {
        case IN:
          inListScans.add(Arrays.asList(scanArray));
          break;

        case EQUAL:
          propertyScans.add(scanArray[0]);
          break;

      }
    }

    buildInListMatrix(inListScans, new Stack<Scan>(), 0, dnfScans);
    if (dnfScans.size() == 0) {
      dnfScans.add(propertyScans);
    }
    else {
      for (List<Scan> dnfScan : dnfScans) {
        dnfScan.addAll(propertyScans);
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
        //todo: this doesn't check for duplicates; figure out a way to eliminate them
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
        throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "'IN' operator requires a non zero size list of supported values as filter operand");
      }
      else {
        final List valueList = (List)filter.getValue();
        if(valueList.size() <= 0 || valueList.size() > 300) {
          throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "'IN' operator requires a list (1-300 items) of supported values as filter operand");
        }
        Class firstClass = null;
        for (Object val : valueList) {
          if(val == null) {
            throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "'IN' operator values cannot be null");
          }
          if(firstClass == null) {
            firstClass = val.getClass();
          }
          else if(!val.getClass().equals(firstClass)) {
            throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "all values in an IN list must be of the same type");
          }
        }
        Object listValue = valueList.get(0);
        if(!EntityUtils.isApprovedClass(listValue.getClass())) {
          throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "class '"+listValue.getClass()+"' not a supported type for IN list");
        }
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
