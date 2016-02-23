/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
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
  public void emptyPrefixTest() {
    PrefixList prefixList = new PrefixList(ImmutableList.of(""));
    Assert.assertTrue(prefixList.inList("a"));

    Assert.assertTrue(prefixList.outList(""));
  }

  /**
   * Tests that the {@link PrefixList#PrefixList(List)} constructor constructs items correctly.
   */
  @Test
  public void prefixListTest() {
    PrefixList prefixList = new PrefixList(ImmutableList.of("test", "apple", "sun"));
    Assert.assertTrue(prefixList.inList("test"));
    Assert.assertTrue(prefixList.inList("apple"));
    Assert.assertTrue(prefixList.inList("sun"));

    Assert.assertTrue(prefixList.inList("test123"));
    Assert.assertTrue(prefixList.inList("testing-1012"));
    Assert.assertTrue(prefixList.inList("apple12nmzx91l"));
    Assert.assertTrue(prefixList.inList("sunn1i2080-40mx"));

    Assert.assertFalse(prefixList.outList("test123"));
    Assert.assertFalse(prefixList.outList("testing-1012"));
    Assert.assertFalse(prefixList.outList("apple12nmzx91l"));
    Assert.assertFalse(prefixList.outList("sunn1i2080-40mx"));

    Assert.assertTrue(prefixList.outList("tes"));
    Assert.assertTrue(prefixList.outList("a"));
    Assert.assertTrue(prefixList.outList("s"));
    Assert.assertTrue(prefixList.outList("su"));
    Assert.assertTrue(prefixList.outList("ap"));
    Assert.assertTrue(prefixList.outList(""));
    Assert.assertTrue(prefixList.outList(null));
  }

  /**
   * Tests that the {@link PrefixList#PrefixList(List)} constructor constructs items correctly.
   */
  @Test
  public void prefixListTest2() {
    PrefixList prefixList = new PrefixList("test;apple;sun", ";");
    Assert.assertTrue(prefixList.inList("test"));
    Assert.assertTrue(prefixList.inList("apple"));
    Assert.assertTrue(prefixList.inList("sun"));

    Assert.assertTrue(prefixList.inList("test123"));
    Assert.assertTrue(prefixList.inList("testing-1012"));
    Assert.assertTrue(prefixList.inList("apple12nmzx91l"));
    Assert.assertTrue(prefixList.inList("sunn1i2080-40mx"));

    Assert.assertFalse(prefixList.outList("test123"));
    Assert.assertFalse(prefixList.outList("testing-1012"));
    Assert.assertFalse(prefixList.outList("apple12nmzx91l"));
    Assert.assertFalse(prefixList.outList("sunn1i2080-40mx"));

    Assert.assertTrue(prefixList.outList("tes"));
    Assert.assertTrue(prefixList.outList("a"));
    Assert.assertTrue(prefixList.outList("s"));
    Assert.assertTrue(prefixList.outList("su"));
    Assert.assertTrue(prefixList.outList("ap"));
    Assert.assertTrue(prefixList.outList(""));
    Assert.assertTrue(prefixList.outList(null));
  }

  /**
   * Tests the {@link PrefixList#toString()} method.
   */
  @Test
  public void toStringTest() {
    PrefixList prefixList = new PrefixList(null, ";");
    Assert.assertEquals(prefixList.toString(), "");

    prefixList = new PrefixList("", ";");
    Assert.assertEquals(prefixList.toString(), "");

    prefixList = new PrefixList(";", ";");
    Assert.assertEquals(prefixList.toString(), "");

    prefixList = new PrefixList(" a ; ; b ", ";");
    Assert.assertEquals(prefixList.toString(), "a;b;");

    prefixList = new PrefixList("a/b;c", ";");
    Assert.assertEquals(prefixList.toString(), "a/b;c;");

    prefixList = new PrefixList("a/b;c;", ";");
    Assert.assertEquals(prefixList.toString(), "a/b;c;");
  }
}
