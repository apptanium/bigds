package com.apptanium.api.bigds.datastore;

import java.nio.charset.Charset;

/**
 * @author sgupta
 * @since 1/19/15.
 */
interface DatastoreConstants {

  static final Charset CHARSET = Charset.forName("UTF-8");
  static final String PROPERTIES_COLUMN_FAMILY = "p";
  static final byte[] PROPERTIES_COLUMN_FAMILY_BYTES = PROPERTIES_COLUMN_FAMILY.getBytes(CHARSET);
  static final String METADATA_COLUMN_FAMILY = "m";
  static final byte[] METADATA_COLUMN_FAMILY_BYTES = METADATA_COLUMN_FAMILY.getBytes(CHARSET);

  /**
   * this column in the metadata column family stores the set of index row ids
   */
  static final byte[] METADATA_INDEX_COLUMN_BYTES = "_".getBytes(CHARSET);

  /**
   * used for storing indexes; not for use in the primary object table, but used in the index table
   */
  static final String ID_COLUMN_FAMILY = "i";
  static final byte[] ID_COLUMN_FAMILY_BYTES = ID_COLUMN_FAMILY.getBytes(CHARSET);

  static final String ID_COLUMN_KEY = "k";
  static final byte[] ID_COLUMN_KEY_BYTES = ID_COLUMN_KEY.getBytes(CHARSET);

  static final String ID_COLUMN_PARENT = "p";
  static final byte[] ID_COLUMN_PARENT_BYTES = ID_COLUMN_PARENT.getBytes(CHARSET);
}
