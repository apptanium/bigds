package com.apptanium.api.bigds.entity;

import java.io.Serializable;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public class Text implements Serializable {
  private static final long serialVersionUID = -4835693717794962074L;

  private final String value;

  public Text(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
