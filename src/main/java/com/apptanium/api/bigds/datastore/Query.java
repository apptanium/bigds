package com.apptanium.api.bigds.datastore;

import com.apptanium.api.bigds.entity.Key;

/**
 * @author sgupta
 * @since 1/15/15.
 */
public class Query {

  private final String namespace;
  private final String kind;
  private final Key parent;
  private final boolean keysOnly;
  private final QueryFilter filter;
  private int limit;
  private int offset;

  public Query(String namespace, String kind, Key parent, boolean keysOnly, QueryFilter filter) {
    this.namespace = namespace;
    this.kind = kind;
    this.parent = parent;
    this.keysOnly = keysOnly;
    this.filter = filter;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getKind() {
    return kind;
  }

  public boolean isKeysOnly() {
    return keysOnly;
  }

  public Key getParent() {
    return parent;
  }

  public QueryFilter getFilter() {
    return filter;
  }

  public int getLimit() {
    return limit;
  }

  public Query setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  public int getOffset() {
    return offset;
  }

  public Query setOffset(int offset) {
    this.offset = offset;
    return this;
  }

}
