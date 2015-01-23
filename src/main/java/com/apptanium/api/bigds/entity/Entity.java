package com.apptanium.api.bigds.entity;

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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Entity{");
    sb.append("key=").append(key);
    sb.append(",properties=").append(map);
    sb.append('}');
    return sb.toString();
  }
}
