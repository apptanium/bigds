package com.apptanium.api.bigds.datastore;

import java.util.Collections;
import java.util.List;

/**
 * @author sgupta
 * @since 1/15/15.
 */
public final class CompositeFilter extends QueryFilter {

  private final CompositeOperator operator;
  private final List<PropertyFilter> filters;

  public CompositeFilter(CompositeOperator operator, List<PropertyFilter> filters) {
    this.operator = operator;
    this.filters = Collections.unmodifiableList(filters);
  }

  public CompositeOperator getOperator() {
    return operator;
  }

  public List<PropertyFilter> getFilters() {
    return filters;
  }
}
