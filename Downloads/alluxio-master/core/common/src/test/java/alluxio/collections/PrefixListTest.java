/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

/**
 * Unit tests for {@link PrefixList}.
 */
public final class PrefixListTest {

  /**
   * Tests that the {@link PrefixList#PrefixList(List)} constructor with an empty string constructs
   * items correctly.
   */
  @Test
  public void emptyPrefix() {
    PrefixList prefixList = new PrefixList(ImmutableList.of(""));
    assertTrue(prefixList.inList("a"));

    assertTrue(prefixList.outList(""));
  }

  /**
   * Tests that the {@link PrefixList#PrefixList(List)} constructor constructs items correctly.
   */
  @Test
  public void prefixList() {
    PrefixList prefixList = new PrefixList(ImmutableList.of("test", "apple", "sun"));
    assertTrue(prefixList.inList("test"));
    assertTrue(prefixList.inList("apple"));
    assertTrue(prefixList.inList("sun"));

    assertTrue(prefixList.inList("test123"));
    assertTrue(prefixList.inList("testing-1012"));
    assertTrue(prefixList.inList("apple12nmzx91l"));
    assertTrue(prefixList.inList("sunn1i2080-40mx"));

    assertFalse(prefixList.outList("test123"));
    assertFalse(prefixList.outList("testing-1012"));
    assertFalse(prefixList.outList("apple12nmzx91l"));
    assertFalse(prefixList.outList("sunn1i2080-40mx"));

    assertTrue(prefixList.outList("tes"));
    assertTrue(prefixList.outList("a"));
    assertTrue(prefixList.outList("s"));
    assertTrue(prefixList.outList("su"));
    assertTrue(prefixList.outList("ap"));
    assertTrue(prefixList.outList(""));
    assertTrue(prefixList.outList(null));
  }

  /**
   * Tests that the {@link PrefixList#PrefixList(List)} constructor constructs items correctly.
   */
  @Test
  public void prefixListTest2() {
    PrefixList prefixList = new PrefixList("test;apple;sun", ";");
    assertTrue(prefixList.inList("test"));
    assertTrue(prefixList.inList("apple"));
    assertTrue(prefixList.inList("sun"));

    assertTrue(prefixList.inList("test123"));
    assertTrue(prefixList.inList("testing-1012"));
    assertTrue(prefixList.inList("apple12nmzx91l"));
    assertTrue(prefixList.inList("sunn1i2080-40mx"));

    assertFalse(prefixList.outList("test123"));
    assertFalse(prefixList.outList("testing-1012"));
    assertFalse(prefixList.outList("apple12nmzx91l"));
    assertFalse(prefixList.outList("sunn1i2080-40mx"));

    assertTrue(prefixList.outList("tes"));
    assertTrue(prefixList.outList("a"));
    assertTrue(prefixList.outList("s"));
    assertTrue(prefixList.outList("su"));
    assertTrue(prefixList.outList("ap"));
    assertTrue(prefixList.outList(""));
    assertTrue(prefixList.outList(null));
  }

  /**
   * Tests the {@link PrefixList#toString()} method.
   */
  @Test
  public void toStringTest() {
    PrefixList prefixList = new PrefixList(null, ";");
    assertEquals(prefixList.toString(), "");

    prefixList = new PrefixList("", ";");
    assertEquals(prefixList.toString(), "");

    prefixList = new PrefixList(";", ";");
    assertEquals(prefixList.toString(), "");

    prefixList = new PrefixList(" a ; ; b ", ";");
    assertEquals(prefixList.toString(), "a;b;");

    prefixList = new PrefixList("a/b;c", ";");
    assertEquals(prefixList.toString(), "a/b;c;");

    prefixList = new PrefixList("a/b;c;", ";");
    assertEquals(prefixList.toString(), "a/b;c;");
  }
}
