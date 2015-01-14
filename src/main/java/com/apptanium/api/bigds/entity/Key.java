package com.apptanium.api.bigds.entity;


import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * A row key to uniquely identify a row in the data store
 *
 * @author sgupta
 */
public final class Key implements Serializable, Comparable<Key> {
  private static final Charset CHARSET = Charset.forName("UTF-8");

  private final Key parent;
  private final String kind;
  private final String id;

  public Key(@Nullable Key parent, @Nonnull String kind, @Nonnull String id) {
    this.parent = parent;
    this.kind = kind;
    this.id = id;
  }

  public Key getParent() {
    return parent;
  }

  public String getKind() {
    return kind;
  }

  public String getId() {
    return id;
  }

  public Key getChild(@Nonnull String kind, @Nonnull String name) {
    return new Key(this, kind, name);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof Key && createString(this, false).equals(createString((Key) obj, false));
  }

  @Override
  public String toString() {
    return id;
  }

  public byte[] getRowId() {
    return createString(this, false).getBytes(CHARSET);
  }

  @Override
  public int compareTo(@Nonnull Key other) {
    return id.compareTo(other.id);
  }

  public static String createString(@Nonnull Key key, boolean webSafe) {
    StringBuilder builder = new StringBuilder();
    Key current = key;
    do {
      builder.insert(0, current.getId()).insert(0, "=").insert(0, current.getKind()).insert(0, "/");
      current = key.parent;
    }
    while (current != null);
    return webSafe ? Base64.encodeBase64URLSafeString(builder.toString().getBytes(CHARSET)) : builder.toString();
  }

  public static Key createKey(@Nonnull String keyString, boolean isWebSafe) {
    if(isWebSafe) {
      keyString = new String(Base64.decodeBase64(keyString), CHARSET);
    }
    String[] parts = keyString.split("[/]");
    Key current = null;
    for (String part : parts) {
      //account for a quirk of the split process
      if(part.length() == 0) {
        continue;
      }
      int index = part.indexOf('=');
      current = new Key(current, part.substring(0, index), part.substring(index+1));
    }
    return current;
  }
}
