/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.conf;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public final class UtilsTest {
  private static final ImmutableList<String> EMPTY = ImmutableList.of();

  @Test
  public void listPropertyTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz");
    String key = "listPropertyTest.1";
    System.setProperty(key, "foo bar baz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertyCommaTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz");
    String key = "listPropertyTest.2";
    System.setProperty(key, "foo,bar,baz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertyMixModeTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz", "biz");
    String key = "listPropertyTest.2";
    System.setProperty(key, "foo,bar baz\tbiz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertyHeavyFormatTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz", "biz");
    String key = "listPropertyTest.2";
    System.setProperty(key, "foo\n\n\n\t\r\tbar\n\n\n\t\n\r\nbaz\tbiz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertiesMissingTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz");
    ImmutableList<String> out = Utils.getListProperty("this should so be missing!", expected);

    Assert.assertEquals(expected, out);
  }

}
