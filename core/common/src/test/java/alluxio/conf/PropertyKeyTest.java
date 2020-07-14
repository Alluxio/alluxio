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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import alluxio.DefaultSupplier;
import alluxio.conf.PropertyKey;
import alluxio.conf.PropertyKey.Builder;
import alluxio.conf.PropertyKey.Template;
import alluxio.conf.RemovedKey;
import alluxio.exception.ExceptionMessage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

/**
 * Tests enum type {@link PropertyKey}.
 */
public final class PropertyKeyTest {

  private PropertyKey mTestProperty = new Builder("alluxio.test.property")
      .setAlias(new String[] {"alluxio.test.property.alias1", "alluxio.test.property.alias2"})
      .setDescription("test")
      .setDefaultValue(false)
      .setIsHidden(false)
      .setIgnoredSiteProperty(false)
      .build();

  @After
  public void after() {
    PropertyKey.unregister(mTestProperty);
  }

  /**
   * Tests parsing string to PropertyKey by {@link PropertyKey#fromString}.
   */
  @Test
  public void fromString() throws Exception {
    assertEquals(PropertyKey.VERSION, PropertyKey.fromString(PropertyKey.VERSION.toString()));
    PropertyKey.fromString(PropertyKey.VERSION.toString());
    assertEquals(mTestProperty, PropertyKey.fromString("alluxio.test.property.alias1"));
    assertEquals(mTestProperty, PropertyKey.fromString("alluxio.test.property.alias2"));
    assertEquals(mTestProperty, PropertyKey.fromString(mTestProperty.toString()));
  }

  @Test
  public void equalsTest() throws Exception {
    assertEquals(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo"),
        PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo"));
    assertEquals(PropertyKey.HOME, PropertyKey.HOME);
  }

  @Test
  public void length() throws Exception {
    assertEquals(PropertyKey.Name.HOME.length(), PropertyKey.HOME.length());
  }

  @Test
  public void isValid() throws Exception {
    assertTrue(PropertyKey.isValid(PropertyKey.HOME.toString()));
    assertTrue(PropertyKey
        .isValid(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo").toString()));
    assertFalse(PropertyKey.isValid(""));
    assertFalse(PropertyKey.isValid(" "));
    assertFalse(PropertyKey.isValid("foo"));
    assertFalse(PropertyKey.isValid(PropertyKey.HOME.toString() + "1"));
    assertFalse(PropertyKey.isValid(PropertyKey.HOME.toString().toUpperCase()));
  }

  @Test
  public void aliasIsValid() throws Exception {
    assertTrue(PropertyKey.isValid(mTestProperty.toString()));
    assertTrue(PropertyKey.isValid("alluxio.test.property.alias1"));
    assertTrue(PropertyKey.isValid("alluxio.test.property.alias2"));
  }

  @Test
  public void fromStringExceptionThrown() throws Exception {
    String[] wrongKeys =
        {"", " ", "foo", "alluxio.foo", "alluxio.HOME", "alluxio.master.mount.table.root.alluxio1",
            "alluxio.master.mount.table.alluxio", "alluxio.master.mount.table.foo"};
    for (String key : wrongKeys) {
      try {
        PropertyKey.fromString(key);
        Assert.fail();
      } catch (IllegalArgumentException e) {
        assertEquals(e.getMessage(),
            ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
      }
    }
  }

  @Test
  public void formatMasterTieredStoreGlobalAlias() throws Exception {
    assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS,
        PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0));
    assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL1_ALIAS,
        PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(1));
    assertEquals(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL2_ALIAS,
        PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(2));
  }

  @Test
  public void formatWorkerTieredStoreAlias() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_ALIAS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsPath() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_PATH,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(2));
  }

  @Test
  public void formatWorkerTieredStoreDirsQuota() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_DIRS_QUOTA,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(2));
  }

  @Test
  public void formatWorkerTieredStoreReservedRatio() throws Exception {
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(0));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL1_HIGH_WATERMARK_RATIO,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(1));
    assertEquals(PropertyKey.WORKER_TIERED_STORE_LEVEL2_HIGH_WATERMARK_RATIO,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO.format(2));
  }

  @Test
  public void mountTableRootProperties() throws Exception {
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_ALLUXIO,
        PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
        PropertyKey.Template.MASTER_MOUNT_TABLE_UFS.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY,
        PropertyKey.Template.MASTER_MOUNT_TABLE_READONLY.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED,
        PropertyKey.Template.MASTER_MOUNT_TABLE_SHARED.format("root"));
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION,
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("root"));
  }

  @Test
  public void isValidParameterized() throws Exception {
    // String parameter
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.foo.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.FoO.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.Fo123.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.FoO.alluxio"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.option"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.option.foo"));
    assertTrue(PropertyKey.isValid("alluxio.master.mount.table.root.option.alluxio.foo"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.alluxio"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table..alluxio"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table. .alluxio"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.foo.alluxio1"));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.root.option."));
    assertFalse(PropertyKey.isValid("alluxio.master.mount.table.root.option.foo."));
    // Numeric parameter
    assertTrue(PropertyKey.isValid("alluxio.worker.tieredstore.level1.alias"));
    assertTrue(PropertyKey.isValid("alluxio.worker.tieredstore.level99.alias"));
    assertFalse(PropertyKey.isValid("alluxio.worker.tieredstore.level.alias"));
    assertFalse(PropertyKey.isValid("alluxio.worker.tieredstore.levela.alias"));
  }

  @Test
  public void fromStringParameterized() throws Exception {
    assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_ALLUXIO,
        PropertyKey.fromString("alluxio.master.mount.table.root.alluxio"));
    assertEquals(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.format("foo"),
        PropertyKey.fromString("alluxio.master.mount.table.foo.alluxio"));
  }

  @Test
  public void fromStringParameterizedExceptionThrown() throws Exception {
    String[] wrongKeys = {"alluxio.master.mount.table.root.alluxio1",
        "alluxio.master.mount.table.alluxio", "alluxio.master.mount.table.foo"};
    for (String key : wrongKeys) {
      try {
        PropertyKey.fromString(key);
        Assert.fail();
      } catch (IllegalArgumentException e) {
        assertEquals(e.getMessage(),
            ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage(key));
      }
    }
  }

  @Test
  public void defaultSupplier() throws Exception {
    AtomicInteger x = new AtomicInteger(100);
    PropertyKey key = new Builder("test")
        .setDefaultSupplier(new DefaultSupplier(() -> x.get(), "test description"))
        .build();
    assertEquals("100", key.getDefaultValue());
    x.set(20);
    assertEquals("20", key.getDefaultValue());
    assertEquals("test description", key.getDefaultSupplier().getDescription());
  }

  @Test
  public void isDeprecated() throws Exception {
    assertFalse(PropertyKey.isDeprecated("alluxio.version"));
  }

  @Test
  public void compare() throws Exception {
    assertTrue(PropertyKey.CONF_DIR.compareTo(PropertyKey.DEBUG) < 0);
    assertTrue(PropertyKey.DEBUG.compareTo(PropertyKey.CONF_DIR) > 0);
    assertEquals(0, PropertyKey.DEBUG.compareTo(PropertyKey.DEBUG));
  }

  @Test
  public void templateMatches() throws Exception {
    assertTrue(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.matches(
        "alluxio.master.mount.table.root.alluxio"));
    assertTrue(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.matches(
        "alluxio.master.mount.table.ufs123.alluxio"));
    assertFalse(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.matches(
        "alluxio.master.mount.table..alluxio"));
    assertFalse(PropertyKey.Template.MASTER_MOUNT_TABLE_ALLUXIO.matches(
        "alluxio.master.mount.table.alluxio"));
  }

  @Test
  public void localityTemplates() throws Exception {
    assertTrue(PropertyKey.isValid("alluxio.locality.node"));
    assertTrue(PropertyKey.isValid("alluxio.locality.custom"));

    assertEquals("alluxio.locality.custom", Template.LOCALITY_TIER.format("custom").toString());
  }

  @Test
  public void isBuiltIn() {
    assertTrue(mTestProperty.isBuiltIn());
  }

  @Test
  public void impersonationRegex() {
    // test groups
    String name = "a-A_1.b-B_2@.groups";
    String groups = String.format("alluxio.master.security.impersonation.%s.groups", name);
    Matcher matcher = PropertyKey.Template.MASTER_IMPERSONATION_GROUPS_OPTION.match(groups);
    assertTrue(matcher.matches());
    assertEquals(name, matcher.group(1));

    // test users
    name = "a-A_1.b-B_2@.users";
    String users = String.format("alluxio.master.security.impersonation.%s.users", name);
    matcher = Template.MASTER_IMPERSONATION_USERS_OPTION.match(users);
    assertTrue(matcher.matches());
    assertEquals(name, matcher.group(1));
  }

  @Test
  public void testEmptyKeyDefaults() {
    for (PropertyKey key : PropertyKey.defaultKeys()) {
      assertNotEquals(String.format(
          "Property keys cannot have a default value of \"\". Offending key: %s", key.getName()),
          key.getDefaultValue(), "");
    }
  }

  @Test
  public void testKeyIsDeprecatedOrRemoved() throws Exception {
    assertTrue(PropertyKey.isDeprecated(PropertyKey.TEST_DEPRECATED_KEY.getName()));
    assertTrue(PropertyKey.isDeprecated(PropertyKey.TEST_DEPRECATED_KEY));

    assertTrue(PropertyKey.isDeprecated(Template.TEST_DEPRECATED_TEMPLATE.format("removed")));
    assertTrue(PropertyKey.isRemoved(RemovedKey.Name.TEST_REMOVED_KEY));
  }

  @Test
  public void testGetOrBuildCustom() throws Exception {
    final PropertyKey workerHostName =
        PropertyKey.getOrBuildCustom(PropertyKey.Name.WORKER_HOSTNAME);

    // check reference equality
    assertTrue(PropertyKey.WORKER_HOSTNAME == workerHostName);

    final PropertyKey test = PropertyKey.getOrBuildCustom("test");
    assertEquals(PropertyKey.DisplayType.DEFAULT, test.getDisplayType());
    assertEquals("test", test.getName());
    assertTrue(test == PropertyKey.getOrBuildCustom("test"));

    final PropertyKey accessKey = PropertyKey.getOrBuildCustom("test.accessKeyId");
    assertEquals(PropertyKey.DisplayType.CREDENTIALS, accessKey.getDisplayType());
  }
}
