package com.apptanium.api.bigds.entity;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public class UnacceptableValueTypeException extends RuntimeException {

  public UnacceptableValueTypeException() {
  }

  public UnacceptableValueTypeException(String message) {
    super(message);
  }

  public UnacceptableValueTypeException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnacceptableValueTypeException(Throwable cause) {
    super(cause);
  }

  public UnacceptableValueTypeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
