package com.apptanium.api.bigds;

import com.apptanium.api.bigds.datastore.DatastoreService;
import com.apptanium.api.bigds.datastore.DatastoreServiceFactory;
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

  //todo: create tests for queries



}
