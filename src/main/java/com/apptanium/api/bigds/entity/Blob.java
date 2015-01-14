package com.apptanium.api.bigds.entity;

import java.io.Serializable;
import java.util.Arrays;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Blob blob = (Blob) o;

    if (!Arrays.equals(value, blob.value)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }
}
