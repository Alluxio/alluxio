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

package alluxio.client.file.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.LoginUserRule;
import alluxio.PropertyKey;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.util.CommonUtils;
import alluxio.wire.TtlAction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Tests for the {@link OutStreamOptions} class.
 */
public class OutStreamOptionsTest {
  /**
   * A mapping from a user to its corresponding group.
   */
  // TODO(binfan): create MockUserGroupsMapping class
  public static class FakeUserGroupsMapping implements GroupMappingService {

    public FakeUserGroupsMapping() {}

    @Override
    public List<String> getGroups(String user) throws IOException {
      return Lists.newArrayList("test_group");
    }
  }

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.class.getName()
  ));

  @Rule
  public LoginUserRule mRule = new LoginUserRule("test_user");

  /**
   * Tests that building an {@link OutStreamOptions} with the defaults works.
   */
  @Test
  public void defaults() throws IOException {
    AlluxioStorageType alluxioType = AlluxioStorageType.STORE;
    UnderStorageType ufsType = UnderStorageType.SYNC_PERSIST;
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH.toString());
    Configuration.set(PropertyKey.USER_FILE_WRITE_TIER_DEFAULT, Constants.LAST_TIER);

    OutStreamOptions options = OutStreamOptions.defaults();

    assertEquals(alluxioType, options.getAlluxioStorageType());
    assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    assertTrue(options.getLocationPolicy() instanceof LocalFirstPolicy);
    assertEquals("test_user", options.getOwner());
    assertEquals("test_group", options.getGroup());
    assertEquals(Mode.defaults().applyFileUMask(), options.getMode());
    assertEquals(Constants.NO_TTL, options.getTtl());
    assertEquals(TtlAction.DELETE, options.getTtlAction());
    assertEquals(ufsType, options.getUnderStorageType());
    assertEquals(WriteType.CACHE_THROUGH, options.getWriteType());
    assertEquals(Constants.LAST_TIER, options.getWriteTier());
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    Random random = new Random();
    long blockSize = random.nextLong();
    FileWriteLocationPolicy locationPolicy = new RoundRobinPolicy();
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());
    long ttl = random.nextLong();
    int writeTier = random.nextInt();
    WriteType writeType = WriteType.NONE;

    OutStreamOptions options = OutStreamOptions.defaults();
    options.setBlockSizeBytes(blockSize);
    options.setLocationPolicy(locationPolicy);
    options.setOwner(owner);
    options.setGroup(group);
    options.setMode(mode);
    options.setTtl(ttl);
    options.setTtlAction(TtlAction.FREE);
    options.setWriteTier(writeTier);
    options.setWriteType(writeType);

    assertEquals(blockSize, options.getBlockSizeBytes());
    assertEquals(locationPolicy, options.getLocationPolicy());
    assertEquals(owner, options.getOwner());
    assertEquals(group, options.getGroup());
    assertEquals(mode, options.getMode());
    assertEquals(ttl, options.getTtl());
    assertEquals(TtlAction.FREE, options.getTtlAction());
    assertEquals(writeTier, options.getWriteTier());
    assertEquals(writeType.getAlluxioStorageType(), options.getAlluxioStorageType());
    assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(OutStreamOptions.class);
  }
}
