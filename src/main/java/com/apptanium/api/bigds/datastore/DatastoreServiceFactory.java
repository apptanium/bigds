package com.apptanium.api.bigds.datastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author sgupta
 * @since 1/10/15.
 */
public class DatastoreServiceFactory {
  private final Random random = new Random();
  private final Configuration configuration;
  private final ExecutorService executorService;
  @Nonnull
  private final String namespace;
  private final int poolSize;
  private Connection[] connections;

  private static DatastoreServiceFactory instance;

  public static void initialize(@Nonnull String namespace, int poolSize, Configuration configuration) throws IOException {
    if(instance == null) {
      instance = new DatastoreServiceFactory(namespace, poolSize, configuration);
    }
    else {
      throw new RuntimeException("instance already initialized");
    }
  }

  public static DatastoreServiceFactory getInstance() {
    return instance;
  }

  /**
   * a convenience method equivalent to getInstance().getDatastoreService();
   * @return
   * @throws IOException
   */
  public static DatastoreService service() throws IOException {
    return getInstance().getDatastoreService();
  }

  private DatastoreServiceFactory(@Nonnull String namespace, int poolSize, Configuration configuration) throws IOException {
    this.namespace = namespace;
    this.poolSize = poolSize;
    this.configuration = HBaseConfiguration.create(configuration);
    this.executorService = Executors.newCachedThreadPool();
    resetConnections();
    try {
      NamespaceDescriptor namespaceDescriptor = getAdmin().getNamespaceDescriptor(namespace);
    }
    catch (NamespaceNotFoundException e) {
      getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());
    }
  }

  public DatastoreService getDatastoreService() throws IOException {
    return new DatastoreServiceImpl(namespace, getConnectionFromPool());
  }

  public Admin getAdmin() throws IOException {
    return getConnectionFromPool().getAdmin();
  }

  private Connection getConnectionFromPool() {
    return this.connections[random.nextInt(connections.length)];
  }

  public synchronized void resetConnections() throws IOException {
    this.connections = new Connection[poolSize];
    for (int i = 0; i < connections.length; i++) {
      connections[i] = ConnectionFactory.createConnection(configuration, executorService);
    }
  }

  public synchronized void closeConnections() throws IOException {
    for (int i = 0; i < connections.length; i++) {
      Connection connection = connections[i];
      connection.close();
    }
  }
}
