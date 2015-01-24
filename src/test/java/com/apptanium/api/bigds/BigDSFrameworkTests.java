package com.apptanium.api.bigds;

import com.apptanium.api.bigds.datastore.DatastoreService;
import com.apptanium.api.bigds.datastore.DatastoreServiceFactory;
import com.apptanium.api.bigds.entity.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Unit test for simple App.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = {KeyTests.class, DatastoreServiceTests.class})
public class BigDSFrameworkTests {

  public static final List<Key> keys = new ArrayList<>(100);

  @BeforeClass
  public static void setupDatastoreServiceFactory() throws IOException {
    DatastoreServiceFactory.initialize("BigDSFrameworkTests", 2, new Configuration());
    System.out.println("initialized DSF!");

    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();

    String[] firstNames = new String[]{"Saurabh", "Jim", "Tim"};
    String[] lastNames = new String[]{"Gupta", "Squaa", "Sqee", "Broo"};

    for (int i = 0; i < 100; i++) {
      String id = EntityUtils.generateId();
      Entity entity = new Entity(new Key(null, "Person", id));
      entity.put("firstName", firstNames[i % firstNames.length]);
      entity.put("lastName", lastNames[i % lastNames.length]);
      entity.put("number", 12300L + i);
      entity.put("isGood", (i % 2 == 0));
      entity.put("intro", new Text("this is a very large piece of text that goes on and on and doesn't stop and all that sort of thing"));
      entity.put("now", new Date(System.currentTimeMillis() + (10000 * i)));
      entity.put("blob", new Blob("BLOB::abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(Charset.forName("UTF-8"))));
      entity.put("shortBlob", new ShortBlob("shortblob_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(Charset.forName("UTF-8"))));
      entity.put("testkey", entity.getKey());

      Key key = datastoreService.put(entity);
      keys.add(key);
      System.out.println("entity = " + entity);
    }

  }

  @AfterClass
  public static void stopDatastoreServiceFactory() throws IOException {
    DatastoreService datastoreService = DatastoreServiceFactory.getInstance().getDatastoreService();
    datastoreService.delete(keys);

    Entity checkEntity = datastoreService.get(keys.get(0));
    assert checkEntity == null;
    System.out.println("deleted all");

    DatastoreServiceFactory.getInstance().closeConnections();
  }

}
