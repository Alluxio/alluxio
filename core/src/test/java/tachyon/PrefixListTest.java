/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Unit tests for {@link PrefixList}
 */
public final class PrefixListTest {

  @Test
  public void emptyPrefixTest() {
    PrefixList prefixList = new PrefixList(ImmutableList.<String> of(""));
    Assert.assertTrue(prefixList.inList("a"));

    Assert.assertTrue(prefixList.outList(""));
  }

  @Test
  public void prefixListTest() {
    PrefixList prefixList = new PrefixList(ImmutableList.<String> of("test", "apple", "sun"));
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
