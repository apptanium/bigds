package com.apptanium.api.bigds;

import com.apptanium.api.bigds.datastore.DatastoreServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = {KeyTests.class, DatastoreServiceTests.class})
public class BigDSFrameworkTests {

  @BeforeClass
  public static void setupDatastoreServiceFactory() throws IOException {
    DatastoreServiceFactory.initialize(2, new Configuration());
    System.out.println("initialized DSF!");
  }

  @AfterClass
  public static void stopDatastoreServiceFactory() throws IOException {
    DatastoreServiceFactory.getInstance().closeConnections();
  }

}
