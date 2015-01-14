package com.apptanium.api.bigds.entity;

import javafx.scene.control.DateCell;
import org.apache.hadoop.hbase.client.Table;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.*;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public abstract class PropertyMap implements Serializable {

  protected final Map<String,Value<? extends Serializable>> map;

  protected PropertyMap() {
    map = new HashMap<>();
  }

  /**
   * equivalent to #put(key, value, true)
   * @param key
   * @param value
   */
  public void put(String key, Serializable value) {
    put(key, value, true);
  }

  public <T extends Serializable> void put(@Nonnull String key, @Nonnull T value, boolean indexed) {
    if(EntityUtils.isApprovedClass(value.getClass())) {
      map.put(key, new Value<T>(value, indexed, (Class<T>) value.getClass()));
    }
    else {
      throw new UnacceptableValueTypeException("["+value.getClass()+"] for supplied value is not an acceptable type for storage");
    }
  }

  public Object remove(@Nonnull String key) {
    return map.remove(key);
  }

  public Object get(@Nonnull String key) {
    Value value = getValue(key);
    return value == null ? null : value.getValue();
  }

  public Value<?> getValue(@Nonnull String key) {
    return map.get(key);
  }

  public Set<String> getPropertyKeys() {
    return map.keySet();
  }

  public static class Value<T> {
    private final T value;
    private final boolean indexed;
    private final Class<T> valueClass;

    private Value(T value, boolean indexed, Class<T> valueClass) {
      this.value = value;
      this.indexed = indexed;
      this.valueClass = valueClass;
    }

    public T getValue() {
      return value;
    }

    public boolean isIndexed() {
      return indexed;
    }

    public Class<T> getValueClass() {
      return valueClass;
    }
  }


}
