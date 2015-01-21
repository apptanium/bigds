package com.apptanium.api.bigds.entity;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * @author sgupta
 * @since 1/8/15.
 */
public class EntityUtils {
  public static final Logger LOGGER = Logger.getLogger(EntityUtils.class.getName());
  private static final Random RANDOM = new SecureRandom();
  private static final char[] ID_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final HashMap<Class<? extends Serializable>, ValueConverter<?>> acceptableValueClasses = new HashMap<>();
  private static final HashMap<Byte, ValueConverter<?>> converterLookup = new HashMap<>();
  static {
    acceptableValueClasses.put(String.class,      new StringConverter());       //S
    acceptableValueClasses.put(Long.class,        new LongConverter());         //L
    acceptableValueClasses.put(Double.class,      new DoubleConverter());       //D
    acceptableValueClasses.put(Boolean.class,     new BooleanConverter());      //B
    acceptableValueClasses.put(Date.class,        new DateConverter());         //A
    acceptableValueClasses.put(Text.class,        new TextConverter());         //T
    acceptableValueClasses.put(ShortBlob.class,   new ShortBlobConverter());    //H
    acceptableValueClasses.put(Blob.class,        new BlobConverter());         //Z
    acceptableValueClasses.put(Key.class,         new KeyConverter());          //K
    acceptableValueClasses.put(EmbeddedMap.class, new EmbeddedMapConverter());  //E

    for (Map.Entry<Class<? extends Serializable>, ValueConverter<?>> converterEntry : acceptableValueClasses.entrySet()) {
      converterLookup.put(converterEntry.getValue().getPrefix(), converterEntry.getValue());
    }
  }

  public static boolean isApprovedClass(Class clazz) {
    return acceptableValueClasses.containsKey(clazz);
  }

  public static ValueConverter getValueConverter(Class clazz) {
    return acceptableValueClasses.get(clazz);
  }

  public static ValueConverter getValueConverter(byte type) {
    return converterLookup.get(type);
  }

  public static String generateId() {
    char[] suffix = new char[8];
    for (int i = 0; i < suffix.length; i++) {
      suffix[i] = ID_CHARS[RANDOM.nextInt(ID_CHARS.length)];
    }
    return Long.toString(System.currentTimeMillis(), 36) + new String(suffix);
  }

  /**
   *
   */
  private static final class StringConverter implements ValueConverter<String> {

    @Override
    public byte[] convertToStorage(String value) {
      if(value.length() > 256) {
        throw new UnacceptableValueException("String values cannot be more than 256 chars in length; use Text for long strings");
      }
      return value.getBytes(CHARSET);
    }

    @Override
    public String convertFromStorage(byte[] bytes) {
      return new String(bytes, CHARSET);
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, String value) {
      return (column+"="+value+"/"+key.getId()).getBytes(CHARSET);
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'S';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, String value) {
      return (column+"="+value+"/").getBytes(CHARSET);
    }
  }

  /**
   *
   */
  private static final class LongConverter implements ValueConverter<Long> {

    @Override
    public byte[] convertToStorage(Long value) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(value);
      return buffer.array();
    }

    @Override
    public Long convertFromStorage(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.put(bytes, 0, bytes.length);
      buffer.flip();
      return buffer.getLong();
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, Long value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column+"=").getBytes(CHARSET));
        ByteBuffer valueBuffer = ByteBuffer.allocate(Long.BYTES);
        valueBuffer.putLong(value);
        buffer.write(valueBuffer.array());
        buffer.write(("/"+key.getId()).getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from Long", e);
      }
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'L';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, Long value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column+"=").getBytes(CHARSET));
        ByteBuffer valueBuffer = ByteBuffer.allocate(Long.BYTES);
        valueBuffer.putLong(value);
        buffer.write(valueBuffer.array());
        buffer.write(("/").getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from Long", e);
      }
    }
  }

  /**
   *
   */
  public static final class DoubleConverter implements ValueConverter<Double> {

    @Override
    public byte[] convertToStorage(Double value) {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
      buffer.putDouble(value);
      return buffer.array();
    }

    @Override
    public Double convertFromStorage(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
      buffer.put(bytes, 0, bytes.length);
      buffer.flip();
      return buffer.getDouble();
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, Double value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column+"=").getBytes(CHARSET));
        ByteBuffer valueBuffer = ByteBuffer.allocate(Double.BYTES);
        valueBuffer.putDouble(value);
        buffer.write(valueBuffer.array());
        buffer.write(("/"+key.getId()).getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from Double", e);
      }
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'D';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, Double value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column+"=").getBytes(CHARSET));
        ByteBuffer valueBuffer = ByteBuffer.allocate(Double.BYTES);
        valueBuffer.putDouble(value);
        buffer.write(valueBuffer.array());
        buffer.write(("/").getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from Double", e);
      }
    }
  }

  /**
   *
   */
  public static final class BooleanConverter implements ValueConverter<Boolean> {

    @Override
    public byte[] convertToStorage(Boolean value) {
      return value ? new byte[]{1} : new byte[]{0};
    }

    @Override
    public Boolean convertFromStorage(byte[] bytes) {
      return bytes[0]==1;
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, Boolean value) {
      return (column+"="+(value?1:0)+"/"+key.getId()).getBytes(CHARSET);
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'B';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, Boolean value) {
      return (column+"="+(value?1:0)+"/").getBytes(CHARSET);
    }
  }

  /**
   *
   */
  public static final class DateConverter implements ValueConverter<Date> {

    @Override
    public byte[] convertToStorage(Date value) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(value.getTime());
      return buffer.array();
    }

    @Override
    public Date convertFromStorage(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.put(bytes, 0, bytes.length);
      buffer.flip();
      return new Date(buffer.getLong());
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, Date value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column+"=").getBytes(CHARSET));
        ByteBuffer valueBuffer = ByteBuffer.allocate(Long.BYTES);
        valueBuffer.putLong(value.getTime());
        buffer.write(valueBuffer.array());
        buffer.write(("/"+key.getId()).getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from Date", e);
      }
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'A';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, Date value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column+"=").getBytes(CHARSET));
        ByteBuffer valueBuffer = ByteBuffer.allocate(Long.BYTES);
        valueBuffer.putLong(value.getTime());
        buffer.write(valueBuffer.array());
        buffer.write(("/").getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from Date", e);
      }
    }
  }

  /**
   *
   */
  public static final class TextConverter implements ValueConverter<Text> {

    @Override
    public byte[] convertToStorage(Text value) {
      return value.getValue().getBytes(CHARSET);
    }

    @Override
    public Text convertFromStorage(byte[] bytes) {
      return new Text(new String(bytes, CHARSET));
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, Text value) {
      return null;
    }

    @Override
    public boolean isIndexable() {
      return false;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'T';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, Text value) {
      return null;
    }
  }

  /**
   *
   */
  public static final class ShortBlobConverter implements ValueConverter<ShortBlob> {

    @Override
    public byte[] convertToStorage(ShortBlob value) {
      if(value.getValue().length > 256) {
        throw new UnacceptableValueException("String values cannot be more than 256 chars in length; use Text for long strings");
      }
      return value.getValue();
    }

    @Override
    public ShortBlob convertFromStorage(byte[] bytes) {
      return new ShortBlob(bytes);
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, ShortBlob value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column + "=").getBytes(CHARSET));
        buffer.write(value.getValue());
        buffer.write(("/"+key.getId()).getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from ShortBlob", e);
      }
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'H';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, ShortBlob value) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try {
        buffer.write((column + "=").getBytes(CHARSET));
        buffer.write(value.getValue());
        buffer.write(("/").getBytes(CHARSET));
        return buffer.toByteArray();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("error while generating indexed row id from ShortBlob", e);
      }
    }
  }

  /**
   *
   */
  public static final class BlobConverter implements ValueConverter<Blob> {

    @Override
    public byte[] convertToStorage(Blob value) {
      return value.getValue();
    }

    @Override
    public Blob convertFromStorage(byte[] bytes) {
      return new Blob(bytes);
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, Blob value) {
      return null;
    }

    @Override
    public boolean isIndexable() {
      return false;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'Z';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, Blob value) {
      return null;
    }
  }

  /**
   *
   */
  public static final class KeyConverter implements ValueConverter<Key> {

    @Override
    public byte[] convertToStorage(Key value) {
      return Key.createString(value, false).getBytes(CHARSET);
    }

    @Override
    public Key convertFromStorage(byte[] bytes) {
      return Key.createKey(new String(bytes, CHARSET), false);
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, Key value) {
      return (column+"="+Key.createString(value, false)+"/"+key.getId()).getBytes(CHARSET);
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'K';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, Key value) {
      return (column+"="+Key.createString(value, false)+"/").getBytes(CHARSET);
    }
  }

  /**
   *
   */
  public static final class EmbeddedMapConverter implements ValueConverter<EmbeddedMap> {

    @Override
    public byte[] convertToStorage(EmbeddedMap value) {
      ByteOutputStream byteOutputStream = new ByteOutputStream();
      try {
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
        objectOutputStream.writeObject(value);
        return byteOutputStream.getBytes();
      }
      catch (IOException e) {
        throw new UnacceptableValueException("Unable to convert EmbeddedMap to storage bytes", e);
      }
    }

    @Override
    public EmbeddedMap convertFromStorage(byte[] bytes) {
      ByteInputStream byteInputStream = new ByteInputStream(bytes, 0, bytes.length);
      try {
        ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream);
        return (EmbeddedMap) objectInputStream.readObject();
      }
      catch (IOException | ClassNotFoundException e) {
        throw new UnacceptableValueException("Unable to convert storage bytes to EmbeddedMap", e);
      }
    }

    @Override
    public byte[] convertToIndexedRowId(Key key, String column, EmbeddedMap value) {
      return null;
    }

    @Override
    public boolean isIndexable() {
      return false;
    }

    @Override
    public Byte getPrefix() {
      return (byte) 'E';
    }

    @Override
    public byte[] convertToRowPrefixId(String column, EmbeddedMap value) {
      return null;
    }
  }

}
