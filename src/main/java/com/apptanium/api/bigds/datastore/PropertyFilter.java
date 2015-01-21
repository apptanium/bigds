package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.EntityUtils;

import java.util.List;

/**
 * @author sgupta
 * @since 1/15/15.
 */
public final class PropertyFilter extends QueryFilter {
  private final String propertyName;
  private final Operator operator;
  private final Object value;

  public PropertyFilter(String propertyName, Operator operator, Object value) {
    this.propertyName = propertyName;
    this.operator = operator;
    this.value = value;
    if(operator == Operator.IN) {
      if(!List.class.isAssignableFrom(value.getClass())) {
        throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "IN operator requires a non-zero list of storable, indexable values");
      }
      List list = (List) value;
      if(list.size() == 0) {
        throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "IN operator requires a non-zero list of storable, indexable values");
      }
      Object item = list.get(0);
      if(!EntityUtils.isApprovedClass(item.getClass())) {
        throw new DatastoreException(DatastoreExceptionCode.InvalidQueryParameter, "IN operator requires a non-zero list of storable, indexable values");
      }
    }
  }

  public String getPropertyName() {
    return propertyName;
  }

  public Operator getOperator() {
    return operator;
  }

  public Object getValue() {
    return value;
  }
}
