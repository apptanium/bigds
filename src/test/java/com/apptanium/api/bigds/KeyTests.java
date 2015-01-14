package com.apptanium.api.bigds;

import com.apptanium.api.bigds.entity.Key;
import org.junit.Test;

/**
 * Created by sgupta on 1/8/15.
 */
public class KeyTests {


  @Test
  public void keyRawStringTest() {
    Key firstKey = new Key(null, "test", "123");
    String rawString = Key.createString(firstKey, false);
    System.out.println("rawString = " + rawString);
    assert rawString.equals("/test=123");

    Key secondKey = Key.createKey(rawString, false);
    assert secondKey.equals(firstKey);
    assert secondKey.getKind().equals(firstKey.getKind());
    assert secondKey.getId().equals(firstKey.getId());
  }

  @Test
  public void keyEncodedStringTest() {
    Key firstKey = new Key(null, "test", "123");
    String rawString = Key.createString(firstKey, true);
    System.out.println("rawString = " + rawString);
    assert rawString.equals("L3Rlc3Q9MTIz");

    Key secondKey = Key.createKey(rawString, true);
    assert secondKey.equals(firstKey);
    assert secondKey.getKind().equals(firstKey.getKind());
    assert secondKey.getId().equals(firstKey.getId());
  }

  public void keyEqualityTest() {

  }

  public void keyHierarchyTest() {

  }

}
