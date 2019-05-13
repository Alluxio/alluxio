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

package alluxio.conf;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Unit tests for the {@link AlluxioProperties} class.
 */
public class AlluxioPropertiesTest {

  private AlluxioProperties mProperties = new AlluxioProperties();
  private PropertyKey mKeyWithValue;
  private PropertyKey mKeyWithoutValue;

  @Before
  public void before() {
    mKeyWithValue = new PropertyKey.Builder("key.with.value").setDefaultValue("value").build();
    mKeyWithoutValue = new PropertyKey.Builder("key.without.value").build();
  }

  @After
  public void after() {
    PropertyKey.unregister(mKeyWithValue);
    PropertyKey.unregister(mKeyWithoutValue);
  }

  @Test
  public void get() {
    assertEquals("value", mProperties.get(mKeyWithValue));
    assertEquals(null, mProperties.get(mKeyWithoutValue));
    mProperties.put(mKeyWithoutValue, "newValue1", Source.RUNTIME);
    assertEquals("newValue1", mProperties.get(mKeyWithoutValue));
  }

  @Test
  public void clear() {
    mProperties.put(mKeyWithValue, "ignored1", Source.RUNTIME);
    mProperties.put(mKeyWithoutValue, "ignored2", Source.RUNTIME);
    mProperties.clear();
    assertEquals(null, mProperties.get(mKeyWithoutValue));
    assertEquals("value", mProperties.get(mKeyWithValue));
  }

  @Test
  public void put() {
    mProperties.put(mKeyWithValue, "value1", Source.SYSTEM_PROPERTY);
    mProperties.put(mKeyWithoutValue, "value2", Source.SYSTEM_PROPERTY);
    assertEquals("value1", mProperties.get(mKeyWithValue));
    assertEquals("value2", mProperties.get(mKeyWithoutValue));

    mProperties.put(mKeyWithValue, "valueLowerPriority", Source.siteProperty(""));
    assertEquals("value1", mProperties.get(mKeyWithValue));
    mProperties.put(mKeyWithValue, "valueSamePriority", Source.SYSTEM_PROPERTY);
    assertEquals("valueSamePriority", mProperties.get(mKeyWithValue));
    mProperties.put(mKeyWithValue, "valueHigherPriority", Source.RUNTIME);
    assertEquals("valueHigherPriority", mProperties.get(mKeyWithValue));
  }

  @Test
  public void remove() {
    mProperties.remove(mKeyWithValue);
    assertEquals(mKeyWithValue.getDefaultValue(), mProperties.get(mKeyWithValue));
    assertEquals(Source.DEFAULT, mProperties.getSource(mKeyWithValue));
  }

  @Test
  public void isSet() {
    assertTrue(mProperties.isSet(mKeyWithValue));
    assertFalse(mProperties.isSet(mKeyWithoutValue));
    mProperties.remove(mKeyWithValue);
    mProperties.put(mKeyWithoutValue, "value", Source.RUNTIME);
    assertTrue(mProperties.isSet(mKeyWithValue));
    assertTrue(mProperties.isSet(mKeyWithoutValue));
  }

  @Test
  public void entrySet() {
    Set<Map.Entry<? extends PropertyKey, String>> expected =
        PropertyKey.defaultKeys().stream()
            .map(key -> Maps.immutableEntry(key, key.getDefaultValue())).collect(toSet());
    assertThat(mProperties.entrySet(), is(expected));
    mProperties.put(mKeyWithValue, "value", Source.RUNTIME);
    expected.add(Maps.immutableEntry(mKeyWithValue, "value"));
    assertThat(mProperties.entrySet(), is(expected));
  }

  @Test
  public void keySet() {
    Set<PropertyKey> expected = new HashSet<>(PropertyKey.defaultKeys());
    assertThat(mProperties.keySet(), is(expected));
    PropertyKey newKey = new PropertyKey.Builder("keySetNew").build();
    mProperties.put(newKey, "value", Source.RUNTIME);
    expected.add(newKey);
    assertThat(mProperties.keySet(), is(expected));
  }

  @Test
  public void forEach() {
    Set<PropertyKey> expected = new HashSet<>(PropertyKey.defaultKeys());
    Set<PropertyKey> actual = Sets.newHashSet();
    mProperties.forEach((key, value) -> actual.add(key));
    assertThat(actual, is(expected));

    PropertyKey newKey = new PropertyKey.Builder("forEachNew").build();
    mProperties.put(newKey, "value", Source.RUNTIME);
    Set<PropertyKey> actual2 = Sets.newHashSet();
    mProperties.forEach((key, value) -> actual2.add(key));
    expected.add(newKey);
    assertThat(actual2, is(expected));
  }

  @Test
  public void setGetSource() {
    mProperties.put(mKeyWithValue, "valueIgnored", Source.RUNTIME);
    assertEquals(Source.RUNTIME, mProperties.getSource(mKeyWithValue));
    assertEquals(Source.DEFAULT, mProperties.getSource(mKeyWithoutValue));
  }

  @Test
  public void merge() {
    PropertyKey newKey = new PropertyKey.Builder("mergeNew").setDefaultValue("value3").build();
    Properties sysProp = new Properties();
    sysProp.put(mKeyWithValue, "value1");
    sysProp.put(mKeyWithoutValue, "value2");
    mProperties.merge(sysProp, Source.SYSTEM_PROPERTY);
    assertEquals(Source.SYSTEM_PROPERTY, mProperties.getSource(mKeyWithValue));
    assertEquals(Source.SYSTEM_PROPERTY, mProperties.getSource(mKeyWithoutValue));
    assertEquals(Source.DEFAULT, mProperties.getSource(newKey));
    assertEquals("value1", mProperties.get(mKeyWithValue));
    assertEquals("value2", mProperties.get(mKeyWithoutValue));
    assertEquals("value3", mProperties.get(newKey));
  }

  @Test
  public void hash() {
    String hash0 = mProperties.hash();

    mProperties.set(mKeyWithValue, "new value");
    String hash1 = mProperties.hash();
    Assert.assertNotEquals(hash0, hash1);

    mProperties.remove(mKeyWithValue);
    String hash2 = mProperties.hash();
    Assert.assertEquals(hash0, hash2);

    mProperties.set(mKeyWithValue, "new value");
    String hash3 = mProperties.hash();
    Assert.assertEquals(hash1, hash3);

    mProperties.set(mKeyWithValue, "updated new value");
    String hash4 = mProperties.hash();
    Assert.assertNotEquals(hash0, hash4);
    Assert.assertNotEquals(hash1, hash4);
    Assert.assertNotEquals(hash2, hash4);
    Assert.assertNotEquals(hash3, hash4);

    mProperties.set(mKeyWithoutValue, "value");
    String hash5 = mProperties.hash();
    Assert.assertNotEquals(hash0, hash5);
    Assert.assertNotEquals(hash1, hash5);
    Assert.assertNotEquals(hash2, hash5);
    Assert.assertNotEquals(hash3, hash5);
    Assert.assertNotEquals(hash4, hash5);
  }
}
