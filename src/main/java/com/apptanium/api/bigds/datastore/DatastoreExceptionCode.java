package com.apptanium.api.bigds.datastore;

/**
 * @author sgupta
 * @since 1/12/15.
 */
public enum DatastoreExceptionCode {
  InvalidEntityKey("Entity key is null or invalid"),
  ErrorAdminNotObtained("Could not access admin interface"),
  ErrorPersistingEntity("Could not persist entity"),
  ErrorCreatingTable("Could not create table"),
  ErrorRetrievingEntity("Could not retrieve entity"),
  ;
  private final String message;

  DatastoreExceptionCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
