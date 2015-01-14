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

}
