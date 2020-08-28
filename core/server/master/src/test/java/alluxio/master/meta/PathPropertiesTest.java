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

package alluxio.master.meta;

import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.NoopJournalContext;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link PathProperties}.
 */
public class PathPropertiesTest {
  private static final String ROOT = "/";
  private static final String DIR1 = "/dir1";
  private static final String DIR1_NESTED = "/dir1/nested";
  private static final String DIR2 = "/dir2";

  private static final Map<PropertyKey, String> READ_CACHE_WRITE_CACHE_THROUGH;
  private static final Map<PropertyKey, String> READ_NO_CACHE_WRITE_THROUGH;
  private static final Map<PropertyKey, String> READ_CACHE;
  private static final Map<PropertyKey, String> WRITE_THROUGH;

  static {
    READ_CACHE_WRITE_CACHE_THROUGH = new HashMap<>();
    READ_CACHE_WRITE_CACHE_THROUGH.put(PropertyKey.USER_FILE_READ_TYPE_DEFAULT,
        ReadType.CACHE.toString());
    READ_CACHE_WRITE_CACHE_THROUGH.put(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT,
        WriteType.CACHE_THROUGH.toString());

    READ_NO_CACHE_WRITE_THROUGH = new HashMap<>();
    READ_NO_CACHE_WRITE_THROUGH.put(PropertyKey.USER_FILE_READ_TYPE_DEFAULT,
        ReadType.NO_CACHE.toString());
    READ_NO_CACHE_WRITE_THROUGH.put(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT,
        WriteType.THROUGH.toString());

    READ_CACHE = new HashMap<>();
    READ_CACHE.put(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.CACHE.toString());

    WRITE_THROUGH = new HashMap<>();
    WRITE_THROUGH.put(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.THROUGH.toString());
  }

  @Test
  public void empty() {
    PathProperties properties = new PathProperties();
    Assert.assertTrue(properties.get().isEmpty());
  }

  @Test
  public void add() {
    PathProperties properties = new PathProperties();

    // add new properties
    properties.add(NoopJournalContext.INSTANCE, ROOT, READ_CACHE_WRITE_CACHE_THROUGH);
    properties.add(NoopJournalContext.INSTANCE, DIR1, READ_NO_CACHE_WRITE_THROUGH);
    Map<String, Map<String, String>> got = properties.get();
    Assert.assertEquals(2, got.size());
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(ROOT));
    assertPropertiesEqual(READ_NO_CACHE_WRITE_THROUGH, got.get(DIR1));

    // overwrite existing properties
    properties.add(NoopJournalContext.INSTANCE, DIR1, READ_CACHE_WRITE_CACHE_THROUGH);
    got = properties.get();
    Assert.assertEquals(2, got.size());
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(ROOT));
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(DIR1));

    // merge properties
    properties.add(NoopJournalContext.INSTANCE, ROOT, READ_NO_CACHE_WRITE_THROUGH);
    properties.add(NoopJournalContext.INSTANCE, DIR1_NESTED, READ_CACHE_WRITE_CACHE_THROUGH);
    properties.add(NoopJournalContext.INSTANCE, DIR2, READ_NO_CACHE_WRITE_THROUGH);
    got = properties.get();
    Assert.assertEquals(4, got.size());
    assertPropertiesEqual(READ_NO_CACHE_WRITE_THROUGH, got.get(ROOT));
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(DIR1));
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(DIR1_NESTED));
    assertPropertiesEqual(READ_NO_CACHE_WRITE_THROUGH, got.get(DIR2));
  }

  @Test
  public void remove() {
    PathProperties properties = new PathProperties();

    // remove from empty properties
    properties.remove(NoopJournalContext.INSTANCE, ROOT, new HashSet<>(Arrays.asList(
        PropertyKey.USER_FILE_READ_TYPE_DEFAULT.getName(),
        PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT.getName())));
    properties.removeAll(NoopJournalContext.INSTANCE, DIR1);
    Assert.assertTrue(properties.get().isEmpty());

    properties.add(NoopJournalContext.INSTANCE, ROOT, READ_CACHE_WRITE_CACHE_THROUGH);
    properties.add(NoopJournalContext.INSTANCE, DIR1, READ_NO_CACHE_WRITE_THROUGH);
    Map<String, Map<String, String>> got = properties.get();
    Assert.assertEquals(2, got.size());
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(ROOT));
    assertPropertiesEqual(READ_NO_CACHE_WRITE_THROUGH, got.get(DIR1));

    // remove non-existent paths
    properties.removeAll(NoopJournalContext.INSTANCE, "non-existent");
    properties.remove(NoopJournalContext.INSTANCE, "non-existent", new HashSet<>(Arrays.asList(
        PropertyKey.USER_FILE_READ_TYPE_DEFAULT.getName(),
        PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT.getName())));
    got = properties.get();
    Assert.assertEquals(2, got.size());
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(ROOT));
    assertPropertiesEqual(READ_NO_CACHE_WRITE_THROUGH, got.get(DIR1));

    // remove non-existent keys
    properties.remove(NoopJournalContext.INSTANCE, ROOT, new HashSet<>(Arrays.asList(
        PropertyKey.USER_APP_ID.getName())));
    properties.remove(NoopJournalContext.INSTANCE, DIR1, new HashSet<>(Arrays.asList(
        PropertyKey.UNDERFS_S3_BULK_DELETE_ENABLED.getName())));
    got = properties.get();
    Assert.assertEquals(2, got.size());
    assertPropertiesEqual(READ_CACHE_WRITE_CACHE_THROUGH, got.get(ROOT));
    assertPropertiesEqual(READ_NO_CACHE_WRITE_THROUGH, got.get(DIR1));

    // remove existing keys
    properties.remove(NoopJournalContext.INSTANCE, ROOT, new HashSet<>(Arrays.asList(
        PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT.getName())));
    properties.remove(NoopJournalContext.INSTANCE, DIR1, new HashSet<>(Arrays.asList(
        PropertyKey.USER_FILE_READ_TYPE_DEFAULT.getName())));
    got = properties.get();
    Assert.assertEquals(2, got.size());
    assertPropertiesEqual(READ_CACHE, got.get(ROOT));
    assertPropertiesEqual(WRITE_THROUGH, got.get(DIR1));

    // remove existing paths
    properties.removeAll(NoopJournalContext.INSTANCE, ROOT);
    got = properties.get();
    Assert.assertEquals(1, got.size());
    assertPropertiesEqual(WRITE_THROUGH, got.get(DIR1));
    properties.removeAll(NoopJournalContext.INSTANCE, DIR1);
    got = properties.get();
    Assert.assertEquals(0, got.size());
  }

  private void assertPropertiesEqual(Map<PropertyKey, String> expected,
      Map<String, String> got) {
    Assert.assertEquals(expected.size(), got.size());
    expected.forEach((key, value) -> {
      Assert.assertTrue(got.containsKey(key.getName()));
      Assert.assertEquals(value, got.get(key.getName()));
    });
  }

  @Test
  public void hashEmpty() {
    PathProperties emptyProperties = new PathProperties();
    String hash = emptyProperties.hash();
    Assert.assertNotNull(hash);
    Assert.assertEquals(hash, emptyProperties.hash());
  }

  @Test
  public void hash() {
    PathProperties properties = new PathProperties();
    String hash0 = properties.hash();

    properties.add(NoopJournalContext.INSTANCE, ROOT, READ_CACHE);
    String hash1 = properties.hash();
    Assert.assertNotEquals(hash0, hash1);

    properties.add(NoopJournalContext.INSTANCE, DIR1, READ_CACHE_WRITE_CACHE_THROUGH);
    String hash2 = properties.hash();
    Assert.assertNotEquals(hash0, hash2);
    Assert.assertNotEquals(hash1, hash2);

    Set<String> keys = new HashSet<>();
    keys.add(PropertyKey.USER_FILE_READ_TYPE_DEFAULT.getName());
    properties.remove(NoopJournalContext.INSTANCE, DIR1, keys);
    String hash3 = properties.hash();
    Assert.assertNotEquals(hash0, hash3);
    Assert.assertNotEquals(hash1, hash3);
    Assert.assertNotEquals(hash2, hash3);

    properties.removeAll(NoopJournalContext.INSTANCE, DIR1);
    String hash4 = properties.hash();
    Assert.assertEquals(hash1, hash4);

    properties.removeAll(NoopJournalContext.INSTANCE, ROOT);
    Assert.assertEquals(hash0, properties.hash());
  }
}
