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

import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.LoginUserRule;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.grpc.TtlAction;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Tests for the {@link OutStreamOptions} class.
 */
public class OutStreamOptionsTest {

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

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
  ), mConf);

  @Rule
  public LoginUserRule mRule = new LoginUserRule("test_user", mConf);

  @After
  public void after(){
    mConf = ConfigurationTestUtils.defaults();
  }

  /**
   * Tests that building an {@link OutStreamOptions} with the defaults works.
   */
  @Test
  public void defaults() throws IOException {
    AlluxioStorageType alluxioType = AlluxioStorageType.STORE;
    UnderStorageType ufsType = UnderStorageType.SYNC_PERSIST;
    mConf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    mConf.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH.toString());
    mConf.set(PropertyKey.USER_FILE_WRITE_TIER_DEFAULT, Constants.LAST_TIER);

    OutStreamOptions options = OutStreamOptions.defaults(mConf);

    assertEquals(alluxioType, options.getAlluxioStorageType());
    assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    assertTrue(options.getLocationPolicy() instanceof LocalFirstPolicy);
    // DON'T COMMIT THIS - NEED TO DO SECURITY DESIGN LATER
//    assertEquals("test_user", options.getOwner());
    assertEquals("test_group", options.getGroup());
    assertEquals(ModeUtils.applyFileUMask(Mode.defaults(),
        mConf.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK)), options.getMode());
    assertEquals(Constants.NO_TTL, options.getTtl());
    assertEquals(TtlAction.DELETE, options.getTtlAction());
    assertEquals(ufsType, options.getUnderStorageType());
    assertEquals(WriteType.CACHE_THROUGH, options.getWriteType());
    assertEquals(Constants.LAST_TIER, options.getWriteTier());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    Random random = new Random();
    long blockSize = random.nextLong();
    FileWriteLocationPolicy locationPolicy = new RoundRobinPolicy(mConf);
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());
    long ttl = random.nextLong();
    int writeTier = random.nextInt();
    WriteType writeType = WriteType.NONE;

    OutStreamOptions options = OutStreamOptions.defaults(mConf);
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
    OutStreamOptions o1 = OutStreamOptions.defaults(mConf);
    OutStreamOptions o2 = OutStreamOptions.defaults(mConf);
    assertEquals(o1, o2);
    assertEquals(o1.hashCode(), o2.hashCode());
//    alluxio.test.util.CommonUtils.testEquals(OutStreamOptions.class, new Class[]{
//        AlluxioConfiguration.class}, new Object[]{mConf});
  }
}
