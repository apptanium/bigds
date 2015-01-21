package com.apptanium.api.bigds.datastore;

/**
 * @author sgupta
 * @since 1/15/15.
 */
public abstract class QueryFilter {


  public static enum Operator {
    EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    IN,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    NOT_EQUAL,
  }

  public static enum CompositeOperator {
    AND,
    OR
  }


}
