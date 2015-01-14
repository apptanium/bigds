package com.apptanium.api.bigds.datastore;

/**
 * @author sgupta
 * @since 1/12/15.
 */
public class DatastoreException extends RuntimeException {
  private final DatastoreExceptionCode code;

  public DatastoreException(DatastoreExceptionCode code) {
    super(code.getMessage());
    this.code = code;
  }

  public DatastoreException(DatastoreExceptionCode code, String message) {
    super(message);
    this.code = code;
  }

  public DatastoreException(DatastoreExceptionCode code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public DatastoreException(DatastoreExceptionCode code, Throwable cause) {
    super(cause);
    this.code = code;
  }

  protected DatastoreException(DatastoreExceptionCode code, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
    this.code = code;
  }

  public DatastoreExceptionCode getCode() {
    return code;
  }
}
