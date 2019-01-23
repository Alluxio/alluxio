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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
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
  public void hashCodeSameProperties() {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.MASTER_RPC_PORT, "1000", Source.RUNTIME);
    props.put(PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES, "1000", Source.RUNTIME);
    props.put(PropertyKey.MASTER_WEB_PORT, "1000", Source.RUNTIME);
    props.put(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, "1000", Source.RUNTIME);

    AlluxioProperties props2 = new AlluxioProperties();
    props2.put(PropertyKey.MASTER_RPC_PORT, "1000", Source.RUNTIME);
    props2.put(PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES, "1000", Source.RUNTIME);
    props2.put(PropertyKey.MASTER_WEB_PORT, "1000", Source.RUNTIME);
    props2.put(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, "1000", Source.RUNTIME);

    assertEquals(props.hashCode(), props2.hashCode());
  }

  @Test
  public void hashCodeSamePropertiesDifferentOrder() {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES, "1000", Source.RUNTIME);
    props.put(PropertyKey.MASTER_RPC_PORT, "1000", Source.RUNTIME);
    props.put(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, "1000", Source.RUNTIME);
    props.put(PropertyKey.MASTER_WEB_PORT, "1000", Source.RUNTIME);

    AlluxioProperties props2 = new AlluxioProperties();
    props2.put(PropertyKey.MASTER_RPC_PORT, "1000", Source.RUNTIME);
    props2.put(PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES, "1000", Source.RUNTIME);
    props2.put(PropertyKey.MASTER_WEB_PORT, "1000", Source.RUNTIME);
    props2.put(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, "1000", Source.RUNTIME);

    assertEquals(props.hashCode(), props2.hashCode());
  }

  @Test
  public void hashCodeDifferentProperties() {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.MASTER_RPC_PORT, "100", Source.SYSTEM_PROPERTY);
    AlluxioProperties props2 = new AlluxioProperties();
    props2.put(PropertyKey.MASTER_RPC_PORT, "100", Source.RUNTIME);
    assertNotEquals(props.hashCode(), props2.hashCode());

    props = new AlluxioProperties();
    props.put(PropertyKey.MASTER_RPC_PORT, "1000", Source.RUNTIME);
    props2 = new AlluxioProperties();
    props2.put(PropertyKey.MASTER_RPC_PORT, "100", Source.RUNTIME);
    assertNotEquals(props.hashCode(), props2.hashCode());

    props = new AlluxioProperties();
    props.put(PropertyKey.MASTER_WEB_PORT, "100", Source.RUNTIME);
    props2 = new AlluxioProperties();
    props2.put(PropertyKey.MASTER_RPC_PORT, "100", Source.RUNTIME);
    assertNotEquals(props.hashCode(), props2.hashCode());
  }

  @Test
  public void hashAddThenUpdate() {
    AlluxioProperties props = new AlluxioProperties();
    props.put(PropertyKey.MASTER_RPC_PORT, "100", Source.CLUSTER_DEFAULT);
    AlluxioProperties props2 = new AlluxioProperties();
    props2.put(PropertyKey.MASTER_RPC_PORT, "100", Source.CLUSTER_DEFAULT);
    assertEquals(props.hashCode(), props2.hashCode());
    props2.remove(PropertyKey.MASTER_RPC_PORT);
    assertNotEquals(props.hashCode(), props2.hashCode());
    props2.put(PropertyKey.MASTER_RPC_PORT, "100", Source.CLUSTER_DEFAULT);
    assertEquals(props.hashCode(), props2.hashCode());
    props2.put(PropertyKey.MASTER_RPC_PORT, "100", Source.DEFAULT);
    assertEquals(props.hashCode(), props2.hashCode()); // Shouldn't update the property.
    props2.put(PropertyKey.MASTER_RPC_PORT, "100", Source.siteProperty("test.properties"));
    assertNotEquals(props.hashCode(), props2.hashCode());
    props.put(PropertyKey.MASTER_RPC_PORT, "100", Source.siteProperty("test.properties"));
    assertEquals(props.hashCode(), props2.hashCode());

    props.put(PropertyKey.WEB_THREADS, "0", Source.RUNTIME);
    assertNotEquals(props.hashCode(), props2.hashCode());
    props2.put(PropertyKey.WEB_THREADS, "0", Source.RUNTIME);
    assertEquals(props.hashCode(), props2.hashCode());
  }
}
