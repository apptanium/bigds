package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.*;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by the
 * @author sgupta
 * @since 1/10/15.
 */
class DatastoreServiceImpl implements DatastoreService {
  private static final Logger LOGGER = Logger.getLogger(DatastoreServiceImpl.class.getName());
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final String PROPERTIES_COLUMN_FAMILY = "p";
  private static final byte[] PROPERTIES_COLUMN_FAMILY_BYTES = PROPERTIES_COLUMN_FAMILY.getBytes(CHARSET);
  private static final String METADATA_COLUMN_FAMILY = "m";
  private static final byte[] METADATA_COLUMN_FAMILY_BYTES = METADATA_COLUMN_FAMILY.getBytes(CHARSET);
  private final Connection connection;

  DatastoreServiceImpl(Connection connection) {
    this.connection = connection;
  }

  @Override
  public Key put(Entity entity) {
    final String kind = entity.getKey().getKind();
    final TableName tableName = TableName.valueOf(kind);

    Admin admin = null;
    try {
      admin = connection.getAdmin();
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorAdminNotObtained, e);
    }

    try {
      if(!admin.tableExists(tableName)) {
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor(PROPERTIES_COLUMN_FAMILY));
        descriptor.addFamily(new HColumnDescriptor(METADATA_COLUMN_FAMILY));
        admin.createTable(descriptor);
      }
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorCreatingTable, e);
    }

    try {
      return putEntityToTable(entity, tableName);
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorPersistingEntity, e);
    }
  }

  private Key put(Entity entity, PutContext context) throws IOException {
    final String kind = entity.getKey().getKind();
    final TableName tableName = TableName.valueOf(kind);
    if(!context.kinds.contains(tableName)) {
      try {
        if(!context.admin.tableExists(tableName)) {
          HTableDescriptor descriptor = new HTableDescriptor(tableName);
          descriptor.addFamily(new HColumnDescriptor(PROPERTIES_COLUMN_FAMILY));
          descriptor.addFamily(new HColumnDescriptor(METADATA_COLUMN_FAMILY));
          context.admin.createTable(descriptor);
        }
        context.kinds.add(tableName);
      }
      catch (IOException e) {
        throw new DatastoreException(DatastoreExceptionCode.ErrorCreatingTable, e);
      }
    }
    return putEntityToTable(entity, tableName);
  }

  private Key putEntityToTable(Entity entity, TableName tableName) throws IOException {
    Table table = connection.getTable(tableName);
    Put row = new Put(entity.getKey().getRowId());
    for (String propertyKey : entity.getPropertyKeys()) {
      PropertyMap.Value propertyValue = entity.getValue(propertyKey);
      ValueConverter converter = EntityUtils.getValueConverter(propertyValue.getValueClass());
      byte[] valueBytes = converter.convertToStorage(propertyValue.getValue());
      byte[] metadataBytes = new byte[]{converter.getPrefix(),
                                        (byte) (converter.isIndexable() ? (propertyValue.isIndexed() ? 1: 0 ) : 0)};
      row.add(PROPERTIES_COLUMN_FAMILY_BYTES, propertyKey.getBytes(CHARSET), valueBytes);
      row.add(METADATA_COLUMN_FAMILY_BYTES, propertyKey.getBytes(CHARSET), metadataBytes);
    }

    table.put(row);
    table.close();
    return entity.getKey();
  }

  @Override
  public List<Key> put(Iterable<Entity> entities) {
    PutContext context = null;
    try {
      context = new PutContext(connection.getAdmin());
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorAdminNotObtained, e);
    }
    List<Key> keys = new LinkedList<>();
    try {
      for (Entity entity : entities) {
        keys.add(put(entity, context));
      }
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorPersistingEntity, e);
    }
    return keys;
  }

/*
  @Override
  public List<Key> put(Entity... entities) {
    return put(Arrays.asList(entities));
  }
*/

  @Override
  public Map<Key, Entity> get(Iterable<Key> keys) {
    GetContext context = null;
    try {
      context = new GetContext(connection.getAdmin());
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorAdminNotObtained, e);
    }
    Map<Key,Entity> results = new HashMap<>();
    for (Key key : keys) {
      final TableName tableName = TableName.valueOf(key.getKind());
      if(context.absent.contains(tableName)) {
        continue;
      }
      Table table = context.present.get(tableName);
      if(table == null) {
        try {
          table = connection.getTable(tableName);
          context.present.put(tableName, table);
        }
        catch (IOException e) {
          context.absent.add(tableName);
          continue;
        }
      }

      if(table != null) {
        Entity entity = getEntityFromTable(key, table);
        if(entity != null) {
          results.put(key, entity);
        }
      }
    }
    for (Map.Entry<TableName, Table> entry : context.present.entrySet()) {
      try {
        entry.getValue().close();
      }
      catch (IOException e) {
        //each entry has to be tried to be closed
      }
    }
    return results;
  }

  private Entity getEntityFromTable(Key key, Table table) {
    try {
      Result result = table.get(new Get(key.getRowId()).addFamily(PROPERTIES_COLUMN_FAMILY_BYTES).addFamily(METADATA_COLUMN_FAMILY_BYTES));
      if(result.isEmpty()) {
        return null;
      }
      Map<byte[], byte[]> map = result.getFamilyMap(PROPERTIES_COLUMN_FAMILY_BYTES);
      Map<byte[], byte[]> md = result.getFamilyMap(METADATA_COLUMN_FAMILY_BYTES);
      Entity entity = new Entity(key);
      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
        String propertyName = new String(entry.getKey(), CHARSET);
        byte[] mdBytes = md.get(entry.getKey());
        ValueConverter converter = EntityUtils.getValueConverter(mdBytes[0]);
        Object propertyValue = converter.convertFromStorage(entry.getValue());
        entity.put(propertyName, propertyValue, mdBytes[1]==1);
      }
      return entity;
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorRetrievingEntity, e);
    }
  }

  @Override
  public Entity get(Key key) {
    final TableName tableName = TableName.valueOf(key.getKind());
    Admin admin = null;
    try {
      admin = connection.getAdmin();
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorAdminNotObtained, e);
    }

    Entity entity = null;
    try {
      Table table = connection.getTable(tableName);

      entity = getEntityFromTable(key, table);

      table.close();
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorRetrievingEntity, e);
    }
    return entity;
  }

  @Override
  public void delete(Iterable<Key> keys) {

  }

  @Override
  public void delete(Key... keys) {

  }

  @Override
  public List<Index> getIndexes() {
    return null;
  }

  private final class PutContext {
    private final Set<TableName> kinds = new HashSet<>();
    private final Admin admin;

    private PutContext(Admin admin) {
      this.admin = admin;
    }
  }

  private final class GetContext {
    private final Map<TableName,Table> present = new HashMap<>();
    private final Set<TableName> absent = new HashSet<>();
    private final Admin admin;

    private GetContext(Admin admin) {
      this.admin = admin;
    }
  }
}
