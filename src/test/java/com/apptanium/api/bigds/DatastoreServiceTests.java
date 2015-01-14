package com.apptanium.api.bigds;

import com.apptanium.api.bigds.datastore.DatastoreService;
import com.apptanium.api.bigds.datastore.DatastoreServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

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


}
