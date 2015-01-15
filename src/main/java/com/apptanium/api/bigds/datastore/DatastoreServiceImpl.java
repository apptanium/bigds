package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.*;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.logging.Level;
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

  /**
   * this column in the metadata column family stores the set of index row ids
   */
  private static final byte[] METADATA_INDEX_COLUMN_BYTES = "_".getBytes(CHARSET);

  /**
   * used for storing indexes; not for use in the primary object table, but used in the index table
   */
  private static final String ID_COLUMN_FAMILY = "i";
  private static final byte[] ID_COLUMN_FAMILY_BYTES = ID_COLUMN_FAMILY.getBytes(CHARSET);

  private final String namespace;
  private final Connection connection;

  DatastoreServiceImpl(String namespace, Connection connection) {
    this.namespace = namespace;
    this.connection = connection;
  }

  @Override
  public Key put(Entity entity) {
    final String kind = entity.getKey().getKind();
    final TableName tableName = TableName.valueOf(namespace, kind);

    Admin admin = null;
    try {
      admin = connection.getAdmin();
    }
    catch (IOException e) {
      throw new DatastoreException(DatastoreExceptionCode.ErrorAdminNotObtained, e);
    }

    try {
      if(!admin.tableExists(tableName)) {
        createTable(admin, tableName);
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
    final TableName tableName = TableName.valueOf(namespace, kind);
    if(!context.kinds.contains(tableName)) {
      try {
        if(!context.admin.tableExists(tableName)) {
          createTable(context.admin, tableName);
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

    Set<byte[]> currIndexRows = null;

    try {
      Result currIndexResult = table.get(new Get(entity.getKey().getRowId()).addColumn(METADATA_COLUMN_FAMILY_BYTES, METADATA_INDEX_COLUMN_BYTES));
      byte[] indexData = currIndexResult.getValue(METADATA_COLUMN_FAMILY_BYTES, METADATA_INDEX_COLUMN_BYTES);
      currIndexRows = indexData == null ? null : (Set<byte[]>) SerializationUtils.deserialize(indexData);
    }
    catch (IOException e) {
      //do nothing, the row doesn't exist
    }
    if(currIndexRows == null) {
      currIndexRows = new HashSet<>();
    }

    Map<byte[], Boolean> newIndexRows = new HashMap<>();

    Put row = new Put(entity.getKey().getRowId());
    for (String propertyKey : entity.getPropertyKeys()) {
      PropertyMap.Value propertyValue = entity.getValue(propertyKey);
      ValueConverter converter = EntityUtils.getValueConverter(propertyValue.getValueClass());
      byte[] valueBytes = converter.convertToStorage(propertyValue.getValue());
      byte[] metadataBytes = new byte[]{converter.getPrefix(),
                                        (byte) (converter.isIndexable() ? (propertyValue.isIndexed() ? 1: 0 ) : 0)};
      row.add(PROPERTIES_COLUMN_FAMILY_BYTES, propertyKey.getBytes(CHARSET), valueBytes);
      row.add(METADATA_COLUMN_FAMILY_BYTES, propertyKey.getBytes(CHARSET), metadataBytes);

      if(converter.isIndexable() && propertyValue.isIndexed()) {
        byte[] indexedRowId = converter.convertToIndexedRowId(entity.getKey(), propertyKey, propertyValue.getValue());
        newIndexRows.put(indexedRowId, !currIndexRows.remove(indexedRowId));
      }
    }
    row.add(METADATA_COLUMN_FAMILY_BYTES,
            METADATA_INDEX_COLUMN_BYTES,
            SerializationUtils.serialize(new HashSet<>(newIndexRows.keySet())));


    table.put(row);
    table.close();

    List<Delete> indexesToDelete = new LinkedList<>();
    for (byte[] currIndexRow : currIndexRows) {
      indexesToDelete.add(new Delete(currIndexRow));
    }
    Table indexTable = connection.getTable(TableName.valueOf(namespace, "_" + tableName.getQualifierAsString()));
    if(indexesToDelete.size() > 0) {
      indexTable.delete(indexesToDelete);
    }
    List<Put> indexesToAdd = new LinkedList<>();
    for (Map.Entry<byte[], Boolean> entry : newIndexRows.entrySet()) {
      if(entry.getValue()) {
        indexesToAdd.add(new Put(entry.getKey()).add(ID_COLUMN_FAMILY_BYTES, "key".getBytes(CHARSET), new byte[]{1}));
      }
    }
    if(indexesToAdd.size() > 0) {
      indexTable.put(indexesToAdd);
    }
    indexTable.close();

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
      final TableName tableName = TableName.valueOf(namespace, key.getKind());
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
    context.close();
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
    final TableName tableName = TableName.valueOf(namespace, key.getKind());
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
    DeleteContext context =  new DeleteContext(); //admin not needed
    for (Key key : keys) {
      final TableName tableName = TableName.valueOf(namespace, key.getKind());
      if (context.absent.contains(tableName)) {
        continue;
      }
      Table[] tables = context.tablePairs.get(tableName);
      if (tables == null) {
        try {
          Table table = connection.getTable(tableName);
          Table indexTable = connection.getTable(TableName.valueOf(namespace, "_" + tableName.getQualifierAsString()));
          context.tablePairs.put(tableName, new Table[]{table,indexTable});
        }
        catch (IOException e) {
          context.absent.add(tableName);
          continue;
        }
      }
      if(tables != null) {
        try {
          Result currIndexResult = tables[0].get(new Get(key.getRowId()).addColumn(METADATA_COLUMN_FAMILY_BYTES, METADATA_INDEX_COLUMN_BYTES));
          byte[] indexData = currIndexResult.getValue(METADATA_COLUMN_FAMILY_BYTES, METADATA_INDEX_COLUMN_BYTES);
          Set<byte[]> currIndexRows = (Set<byte[]>) SerializationUtils.deserialize(indexData);

          for (byte[] currIndexRow : currIndexRows) {
            tables[1].delete(new Delete(currIndexRow));
          }
          tables[0].delete(new Delete(key.getRowId()));
        }
        catch (IOException e) {
          //most likely that table was not found, or key doesn't exist; no harm done :)
          LOGGER.log(Level.INFO, e.getLocalizedMessage(), e);
        }
      }
    }
    context.close();

  }

  @Override
  public void delete(Key key) {
    TableName tableName = TableName.valueOf(namespace, key.getKind());
    try {
      Table table = connection.getTable(tableName);
      Table index = connection.getTable(TableName.valueOf(namespace, "_"+tableName.getQualifierAsString()));

      Result currIndexResult = table.get(new Get(key.getRowId()).addColumn(METADATA_COLUMN_FAMILY_BYTES, METADATA_INDEX_COLUMN_BYTES));
      byte[] indexData = currIndexResult.getValue(METADATA_COLUMN_FAMILY_BYTES, METADATA_INDEX_COLUMN_BYTES);
      Set<byte[]> currIndexRows = (Set<byte[]>) SerializationUtils.deserialize(indexData);

      for (byte[] currIndexRow : currIndexRows) {
        index.delete(new Delete(currIndexRow));
      }

      table.delete(new Delete(key.getRowId()));
      table.close();
      index.close();
    }
    catch (IOException e) {
      //most likely that table was not found, or key doesn't exist; no harm done :)
      LOGGER.log(Level.INFO, e.getLocalizedMessage(), e);
    }
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public List<Index> getIndexes() {
    throw new NotImplementedException("not implemented");
  }

  /**
   * create an entity table, and a backing table for its indexed values
   * @param admin
   * @param tableName
   * @throws IOException
   */
  private void createTable(Admin admin, TableName tableName) throws IOException {
    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    descriptor.addFamily(new HColumnDescriptor(PROPERTIES_COLUMN_FAMILY_BYTES));
    descriptor.addFamily(new HColumnDescriptor(METADATA_COLUMN_FAMILY_BYTES));
    admin.createTable(descriptor);

    HTableDescriptor indexDescriptor = new HTableDescriptor(TableName.valueOf(namespace, "_" + tableName.getQualifierAsString()));
    indexDescriptor.addFamily(new HColumnDescriptor(ID_COLUMN_FAMILY_BYTES));
    admin.createTable(indexDescriptor);
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

    private void close() {
      for (Map.Entry<TableName, Table> entry : present.entrySet()) {
        try {
          entry.getValue().close();
        }
        catch (IOException e) {
          //each entry has to be tried to be closed
        }
      }
    }
  }

  private final class DeleteContext {
    private final Map<TableName, Table[]> tablePairs = new HashMap<>();
    private final Set<TableName> absent = new HashSet<>();

    private void close() {
      for (Map.Entry<TableName, Table[]> entry : tablePairs.entrySet()) {
        try {
          entry.getValue()[0].close();
          entry.getValue()[1].close();
        }
        catch (IOException e) {
          //each entry has to be tried to be closed
        }
      }
    }

  }
}
