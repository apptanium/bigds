package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Entity;
import com.apptanium.api.bigds.entity.EntityUtils;
import com.apptanium.api.bigds.entity.Key;
import com.apptanium.api.bigds.entity.ValueConverter;
import org.apache.hadoop.hbase.client.Result;

import java.util.Map;

/**
 * @author sgupta
 * @since 1/20/15.
 */
class DatastoreUtils implements DatastoreConstants {

  static Entity createEntity(Result result, Key key) {
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

  static Key createKeyFromIndexRow(Result result, String kind) {
    byte[] parentBytes = result.getValue(ID_COLUMN_FAMILY_BYTES, ID_COLUMN_PARENT_BYTES);
    byte[] keyIdBytes = result.getValue(ID_COLUMN_FAMILY_BYTES, ID_COLUMN_KEY_BYTES);
    Key parent = parentBytes != null && parentBytes.length > 1 ? Key.createKey(new String(parentBytes, CHARSET), false) : null;
    return new Key(parent, kind, new String(keyIdBytes, DatastoreConstants.CHARSET));
  }
}
