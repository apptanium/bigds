package com.apptanium.api.bigds.entity;

import java.io.Serializable;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public class Blob implements Serializable {
  private static final long serialVersionUID = 556769172504367293L;

  private final byte[] value;

  public Blob(byte[] value) {
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }
}
