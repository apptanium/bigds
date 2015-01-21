package com.apptanium.api.bigds.entity;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public interface ValueConverter<T> {

  public byte[] convertToStorage(T value);

  public T convertFromStorage(byte[] bytes);

  public byte[] convertToIndexedRowId(Key key, String column, T value);

  public boolean isIndexable();

  public Byte getPrefix();

  public byte[] convertToRowPrefixId(String column, T value);
}
