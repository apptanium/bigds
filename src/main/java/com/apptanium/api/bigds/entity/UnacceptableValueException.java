package com.apptanium.api.bigds.entity;

/**
 * @author sgupta
 * @since 1/13/15.
 */
public class UnacceptableValueException extends RuntimeException {

  public UnacceptableValueException() {
  }

  public UnacceptableValueException(String message) {
    super(message);
  }

  public UnacceptableValueException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnacceptableValueException(Throwable cause) {
    super(cause);
  }

  public UnacceptableValueException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
