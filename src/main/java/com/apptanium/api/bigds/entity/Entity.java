package com.apptanium.api.bigds.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sgupta
 *
 */
public final class Entity extends PropertyMap {

  private final Key key;

  public Entity(Key key) {
    this.key = key;
  }


  public Key getKey() {
    return key;
  }


  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || !(obj == null || !(obj instanceof Entity)) && ((Entity) obj).getKey().equals(key);
  }
}
