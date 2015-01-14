package com.apptanium.api.bigds.entity;

import java.io.Serializable;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public class ShortBlob implements Serializable {
  private static final long serialVersionUID = 1019604840720656274L;

  private final byte[] value;

  public ShortBlob(byte[] value) {
    if(value.length > 256) {
      throw new UnacceptableValueException("ShortBlob can be at most 256 bytes in length");
    }
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }
}
