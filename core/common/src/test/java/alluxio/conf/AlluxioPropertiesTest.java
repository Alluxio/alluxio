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

import alluxio.PropertyKey;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Unit tests for the {@link AlluxioProperties} class.
 */
public class AlluxioPropertiesTest {

  private static final String KEY_WITHOUT_VALUE = "key.without.value";
  private static final String KEY_WITH_VALUE = "key.with.value";
  private static final String KEY_NEW = "key.ne1w";

  private AlluxioProperties mProperties = new AlluxioProperties();
  private PropertyKey mKeyWithValue = new PropertyKey.Builder(KEY_WITH_VALUE).setDefaultValue(
      "value").build();
  private PropertyKey mKeyWithoutValue = new PropertyKey.Builder(KEY_WITHOUT_VALUE).build();

  @Test
  public void get() {
    assertEquals("value", mProperties.get(KEY_WITH_VALUE));
    assertEquals(null, mProperties.get(KEY_WITHOUT_VALUE));
    mProperties.put(KEY_WITHOUT_VALUE, "newValue1", Source.RUNTIME);
    mProperties.put(KEY_NEW, "newValue2", Source.RUNTIME);
    assertEquals("newValue1", mProperties.get(KEY_WITHOUT_VALUE));
    assertEquals("newValue2", mProperties.get(KEY_NEW));
  }

  @Test
  public void clear() {
    mProperties.put(KEY_WITH_VALUE, "ignored1", Source.RUNTIME);
    mProperties.put(KEY_WITHOUT_VALUE, "ignored2", Source.RUNTIME);
    mProperties.clear();
    assertEquals(null, mProperties.get(KEY_WITHOUT_VALUE));
    assertEquals("value", mProperties.get(KEY_WITH_VALUE));
  }

  @Test
  public void put() {
    mProperties.put(KEY_WITH_VALUE, "value1", Source.RUNTIME);
    mProperties.put(KEY_WITHOUT_VALUE, "value2", Source.RUNTIME);
    assertEquals("value1", mProperties.get(KEY_WITH_VALUE));
    assertEquals("value2", mProperties.get(KEY_WITHOUT_VALUE));
  }

  @Test
  public void remove() {
    mProperties.remove(KEY_WITH_VALUE);
    assertEquals(null, mProperties.get(KEY_WITH_VALUE));
  }

  @Test
  public void hasValueSet() {
    assertTrue(mProperties.hasValueSet(KEY_WITH_VALUE));
    assertFalse(mProperties.hasValueSet(KEY_WITHOUT_VALUE));
    mProperties.remove(KEY_WITH_VALUE);
    mProperties.put(KEY_WITHOUT_VALUE, "value", Source.RUNTIME);
    assertFalse(mProperties.hasValueSet(KEY_WITH_VALUE));
    assertTrue(mProperties.hasValueSet(KEY_WITHOUT_VALUE));
  }

  @Test
  public void entrySet() {
    Set<Map.Entry<String, String>> expected =
        PropertyKey.defaultKeys().stream()
            .map(key -> Maps.immutableEntry(key.getName(), key.getDefaultValue())).collect(toSet());
    assertThat(mProperties.entrySet(), is(expected));
    mProperties.put(KEY_NEW, "value", Source.RUNTIME);
    expected.add(Maps.immutableEntry(KEY_NEW, "value"));
    assertThat(mProperties.entrySet(), is(expected));
  }

  @Test
  public void keySet() {
    Set<String> expected =
        PropertyKey.defaultKeys().stream().map(PropertyKey::getName).collect(toSet());
    assertThat(mProperties.keySet(), is(expected));
    mProperties.put(KEY_NEW, "value", Source.RUNTIME);
    expected.add(KEY_NEW);
    assertThat(mProperties.keySet(), is(expected));
  }

  @Test
  public void forEach() {
    Set<String> expected =
        PropertyKey.defaultKeys().stream().map(PropertyKey::getName).collect(toSet());
    Set<String> actual = Sets.newHashSet();
    mProperties.forEach((key, value) -> actual.add(key));
    assertThat(actual, is(expected));

    mProperties.put(KEY_NEW, "value", Source.RUNTIME);
    Set<String> actual2 = Sets.newHashSet();
    mProperties.forEach((key, value) -> actual2.add(key));
    expected.add(KEY_NEW);
    assertThat(actual2, is(expected));
  }

  @Test
  public void setGetSource() {
    mProperties.put(KEY_NEW, "value", Source.RUNTIME);
    mProperties.setSource(KEY_NEW, Source.SYSTEM_PROPERTY);
    assertEquals(Source.SYSTEM_PROPERTY, mProperties.getSource(KEY_NEW));
    assertEquals(Source.DEFAULT, mProperties.getSource(KEY_WITH_VALUE));
    assertEquals(Source.DEFAULT, mProperties.getSource(KEY_WITHOUT_VALUE));
  }

  @Test
  public void merge() {
    Map<Source, Properties> newSources = Maps.newHashMap();
    Properties sysProp = new Properties();
    sysProp.put(KEY_NEW, "value1");
    sysProp.put(KEY_WITHOUT_VALUE, "value2");
    newSources.put(Source.SYSTEM_PROPERTY, sysProp);
    mProperties.merge(sysProp, Source.SYSTEM_PROPERTY);
    assertEquals(Source.SYSTEM_PROPERTY, mProperties.getSource(KEY_NEW));
    assertEquals(Source.SYSTEM_PROPERTY, mProperties.getSource(KEY_WITHOUT_VALUE));
    assertEquals(Source.DEFAULT, mProperties.getSource(KEY_WITH_VALUE));
    assertEquals("value1", mProperties.get(KEY_NEW));
    assertEquals("value2", mProperties.get(KEY_WITHOUT_VALUE));
  }
}
