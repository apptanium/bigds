package com.apptanium.api.bigds.entity;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public final class EmbeddedMap extends PropertyMap {

  public EmbeddedMap() {
    super();
  }

  public EmbeddedMap add(String key, Object value, boolean indexed) {
    put(key, value, indexed);
    return this;
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || !(obj == null || !(obj instanceof EmbeddedMap)) && ((EmbeddedMap) obj).map.equals(map);
  }
}
