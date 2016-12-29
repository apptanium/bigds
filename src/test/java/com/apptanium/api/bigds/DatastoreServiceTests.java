package com.apptanium.api.bigds;

import com.apptanium.api.bigds.datastore.*;
import com.apptanium.api.bigds.datastore.Query;
import com.apptanium.api.bigds.entity.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;

/**
 * @author sgupta
 * @since 1/11/15.
 */
public class DatastoreServiceTests {

  @Test
  public void connectionTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.service();
    System.out.println("datastoreService = " + datastoreService);

//    Configuration configuration = HBaseConfiguration.create();
//
//    HBaseAdmin admin = new HBaseAdmin(configuration);
//
    Admin admin = DatastoreServiceFactory.getInstance().getAdmin();

    HTableDescriptor newTableDesc = new HTableDescriptor(TableName.valueOf("BigDSTest"));
    newTableDesc.addFamily(new HColumnDescriptor("p")); // the properties family
    if(!admin.tableExists(newTableDesc.getTableName())) {
      admin.createTable(newTableDesc);
    }
    HTableDescriptor[] descriptors = admin.listTables();
    System.out.println("descriptors = " + Arrays.toString(descriptors));
    for (HTableDescriptor descriptor : descriptors) {
      System.out.println("descriptor.name = " + descriptor.getNameAsString());
      System.out.println("descriptor.tableName = " + descriptor.getTableName());
    }
    admin.disableTable(newTableDesc.getTableName());
    admin.deleteTable(TableName.valueOf("BigDSTest"));

    descriptors = admin.listTables();
    System.out.println("updated descriptors = " + Arrays.toString(descriptors));
    for (HTableDescriptor descriptor : descriptors) {
      System.out.println("descriptor.name = " + descriptor.getNameAsString());
      System.out.println("descriptor.tableName = " + descriptor.getTableName());
    }

  }

  @Test
  public void entityStorageRetrievalTest() throws IOException {
    String id = Long.toString(System.currentTimeMillis(), 36);
    Entity entity = new Entity(new Key(null, "Person", id));
    entity.put("firstName", "Saurabh");
    entity.put("lastName", "Gupta");
    entity.put("number", 123456L);
    entity.put("isGood", true);
    entity.put("intro", new Text("this is a very large piece of text that goes on and on and doesn't stop and all that sort of thing"));
    entity.put("now", new Date());
    entity.put("blob", new Blob("BLOB::abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(Charset.forName("UTF-8"))));
    entity.put("shortBlob", new ShortBlob("shortblob_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(Charset.forName("UTF-8"))));
    entity.put("testkey", entity.getKey());

    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    Key key = datastoreService.put(entity);

    Entity result = datastoreService.get(key);

    assert result.getKey().equals(entity.getKey());
    for (String property : entity.getPropertyKeys()) {
      System.out.println("result.get(property) = " + result.get(property));
      assert result.get(property).equals(entity.get(property));
    }

    Configuration configuration = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(configuration);
    TableName personIndexTableName = TableName.valueOf(datastoreService.getNamespace(), "_" + entity.getKey().getKind());
    Table personIndexTable = connection.getTable(personIndexTableName);
    assert personIndexTable.getName().equals(personIndexTableName);
    ValueConverter converter = EntityUtils.getValueConverter(Boolean.class);
    Result indexResult = personIndexTable.get(new Get(converter.convertToIndexedRowId(entity.getKey(), "isGood", entity.get("isGood"))));
    assert indexResult != null;
    assert !indexResult.isEmpty();

    converter = EntityUtils.getValueConverter(String.class);
    indexResult = personIndexTable.get(new Get(converter.convertToIndexedRowId(entity.getKey(), "lastName", entity.get("lastName"))));
    assert indexResult != null;
    assert !indexResult.isEmpty();


    datastoreService.delete(key);

    Entity deletedResult = datastoreService.get(key);
    assert deletedResult == null;

    indexResult = personIndexTable.get(new Get(converter.convertToIndexedRowId(entity.getKey(), "lastName", entity.get("lastName"))));
    assert indexResult != null;
    assert indexResult.isEmpty();

    personIndexTable.close();

  }

  @Test
  public void entityPropertyFilterNumberEqualsTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter numberFilter = new PropertyFilter("number", QueryFilter.Operator.EQUAL, 12335L);
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, numberFilter);
    CompiledQuery compiled = datastoreService.compile(query);
    QueryResults results = compiled.getResults();
    while (results.hasNext()) {
      Entity next = results.next();
      System.out.println("numberfilter next.getKey().toString() = " + next.getKey());
      assert (next.get("number")).equals(12335L);
    }
    results.close();
  }

  @Test
  public void entityPropertyStringEqualsTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter stringFilter = new PropertyFilter("firstName", QueryFilter.Operator.EQUAL, "Tim");
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, stringFilter);
    CompiledQuery compiled = datastoreService.compile(query);
    QueryResults results = compiled.getResults();
    while (results.hasNext()) {
      Entity entity = results.next();
      System.out.println("stringfilter entity = " + entity);
      assert entity.get("firstName").equals("Tim");
    }
    results.close();
  }

  @Test
  public void entityPropertyStringInListTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();


    QueryFilter inListFilter = new PropertyFilter("firstName", QueryFilter.Operator.IN, Arrays.asList("Jim", "Tim"));
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, inListFilter);
    CompiledQuery inListCompiled = datastoreService.compile(query);
    QueryResults inListResults = inListCompiled.getResults();
    int count = 0;
    while (inListResults.hasNext()) {
      Entity entity = inListResults.next();
//        System.out.println("in list filter entity = " + entity);
      String firstName = (String) entity.get("firstName");
      assert firstName.equals("Tim") || firstName.equals("Jim");
      assert !firstName.equals("Saurabh");
      count++;
    }
    inListResults.close();
    assert count > 10;
    System.out.println("count = " + count);
  }

  @Test
  public void entityPropertyNumberLessThanTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter filter = new PropertyFilter("number", QueryFilter.Operator.LESS_THAN, 12310L);
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, filter);
    CompiledQuery compiledQuery = datastoreService.compile(query);
    QueryResults results = compiledQuery.getResults();
    int count = 0;
    while (results.hasNext()) {
      Entity entity = results.next();
      if (entity == null) {
        continue;
      }
      System.out.println("LESS_THAN result entity = " + entity);
      assert (Long)entity.get("number") < 12310L;
      count++;
    }
    results.close();
    System.out.println("count = " + count);
    assert count == 10;

  }

  @Test
  public void entityPropertyNumberLessThanEqualTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter filter = new PropertyFilter("number", QueryFilter.Operator.LESS_THAN_OR_EQUAL, 12310L);
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, filter);
    CompiledQuery compiledQuery = datastoreService.compile(query);
    QueryResults results = compiledQuery.getResults();
    int count = 0;
    while (results.hasNext()) {
      Entity entity = results.next();
      if (entity == null) {
        continue;
      }
      System.out.println("LESS_THAN_OR_EQUAL result entity = " + entity);
      assert (Long)entity.get("number") <= 12310L;
      count++;
    }
    results.close();
    System.out.println("count = " + count);
    assert count == 11;

  }

  @Test
  public void entityPropertyNumberGreaterThanTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter filter = new PropertyFilter("number", QueryFilter.Operator.GREATER_THAN, 12390L);
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, filter);
    CompiledQuery compiledQuery = datastoreService.compile(query);
    QueryResults results = compiledQuery.getResults();
    int count = 0;
    while (results.hasNext()) {
      Entity entity = results.next();
      if (entity == null) {
        continue;
      }
      System.out.println("GREATER_THAN result entity = " + entity);
      assert (Long)entity.get("number") > 12390L;
      count++;
    }
    results.close();
    System.out.println("count = " + count);
    assert count == 9; //12391 thru 12399 inclusive

  }

  @Test
  public void entityPropertyNumberGreaterThanEqualTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter filter = new PropertyFilter("number", QueryFilter.Operator.GREATER_THAN_OR_EQUAL, 12390L);
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, filter);
    CompiledQuery compiledQuery = datastoreService.compile(query);
    QueryResults results = compiledQuery.getResults();
    int count = 0;
    while (results.hasNext()) {
      Entity entity = results.next();
      if (entity == null) {
        continue;
      }
      System.out.println("GREATER_THAN_OR_EQUAL result entity = " + entity);
      assert (Long)entity.get("number") >= 12390L;
      count++;
    }
    results.close();
    System.out.println("count = " + count);
    assert count == 10; //12390 thru 12399 inclusive
  }

  @Test
  public void entityPropertyStringNotEqualTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter filter = new PropertyFilter("lastName", QueryFilter.Operator.NOT_EQUAL, "Gupta");
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, filter);
    CompiledQuery compiledQuery = datastoreService.compile(query);
    QueryResults results = compiledQuery.getResults();
    int count = 0;
    while (results.hasNext()) {
      Entity entity = results.next();
      if (entity == null) {
        continue;
      }
      System.out.println("NOT_EQUAL string result entity = " + entity);
      assert !entity.get("lastName").equals("Gupta");
      count++;
    }
    results.close();
    System.out.println("count = " + count);
    assert count == 75; //25 each for Squee Sqaa and Broo
  }

  @Test
  public void entityPropertyBooleanTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter filter = new PropertyFilter("isGood", QueryFilter.Operator.EQUAL, Boolean.TRUE);
    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, filter);
    CompiledQuery compiledQuery = datastoreService.compile(query);
    QueryResults results = compiledQuery.getResults();
    int count = 0;
    while (results.hasNext()) {
      Entity entity = results.next();
      if (entity == null) {
        continue;
      }
      System.out.println("EQUAL boolean result entity = " + entity);
      assert entity.get("isGood").equals(Boolean.TRUE);
      count++;
    }
    results.close();
    System.out.println("count = " + count);
    assert count == 50;
  }

  @Test
  public void entityCompositeOrTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    QueryFilter compositeOrFilter =
        new CompositeFilter(QueryFilter.CompositeOperator.OR,
                            Arrays.asList(
                                new PropertyFilter("lastName", QueryFilter.Operator.IN, Arrays.asList("Squaa", "Broo")),
                                new PropertyFilter("number", QueryFilter.Operator.GREATER_THAN, 12390L)
                                         ));

    Query query = new Query(datastoreService.getNamespace(), "Person", null, false, compositeOrFilter);
    CompiledQuery compiledQuery = datastoreService.compile(query);
    QueryResults results = compiledQuery.getResults();
    int count = 0;
    while (results.hasNext()) {
      Entity entity = results.next();
      if(entity == null) {
        continue;
      }
      System.out.println("COMPOSITE OR entity = " + entity);
      assert entity.get("lastName").equals("Squaa") || entity.get("lastName").equals("Broo") || ((Long) entity.get("number") > 12390L);
      count++;
    }
    results.close();
    System.out.println("count = " + count);
    assert count > 10;
  }

  @Test
  public void entityCompositeAndTest() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();


  }


}
