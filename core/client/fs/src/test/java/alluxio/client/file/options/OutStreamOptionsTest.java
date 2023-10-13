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

import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.LocalFirstPolicy;
import alluxio.client.block.policy.RoundRobinPolicy;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.TtlAction;
import alluxio.security.User;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.testing.EqualsTester;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import javax.security.auth.Subject;

/**
 * Tests for the {@link OutStreamOptions} class.
 */
public class OutStreamOptionsTest {

  private InstancedConfiguration mConf = Configuration.copyGlobal();

  /**
   * A mapping from a user to its corresponding group.
   */
  // TODO(binfan): create MockUserGroupsMapping class
  public static class FakeUserGroupsMapping implements GroupMappingService {

    public FakeUserGroupsMapping() {}

    @Override
    public List<String> getGroups(String user) {
      return Lists.newArrayList("test_group");
    }
  }

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.class.getName()
  ), mConf);

  @After
  public void after() {
    mConf = Configuration.copyGlobal();
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
    mConf.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.class.getName());
    Subject subject = new Subject();
    subject.getPrincipals().add(new User("test_user"));
    ClientContext clientContext = ClientContext.create(subject, mConf);

    OutStreamOptions options = OutStreamOptions.defaults(FileSystemContext.create(clientContext));

    assertEquals(alluxioType, options.getAlluxioStorageType());
    assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    assertTrue(options.getLocationPolicy() instanceof LocalFirstPolicy);
    assertEquals("test_user", options.getOwner());
    assertEquals("test_group", options.getGroup());
    assertEquals(ModeUtils.applyFileUMask(Mode.defaults(),
        mConf.getString(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK)), options.getMode());
    assertEquals(Constants.NO_TTL, options.getCommonOptions().getTtl());
    assertEquals(TtlAction.FREE, options.getCommonOptions().getTtlAction());
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
    BlockLocationPolicy locationPolicy = new RoundRobinPolicy(
        Configuration.global());
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());
    int ttl = 5;
    TtlAction ttlAction = TtlAction.FREE;
    int writeTier = random.nextInt();
    WriteType writeType = WriteType.NONE;

    mConf.set(PropertyKey.USER_FILE_CREATE_TTL, ttl);
    mConf.set(PropertyKey.USER_FILE_CREATE_TTL_ACTION, ttlAction);

    ClientContext clientContext = ClientContext.create(mConf);
    OutStreamOptions options = OutStreamOptions.defaults(FileSystemContext.create(clientContext));
    options.setBlockSizeBytes(blockSize);
    options.setLocationPolicy(locationPolicy);
    options.setOwner(owner);
    options.setGroup(group);
    options.setMode(mode);
    options.setWriteTier(writeTier);
    options.setWriteType(writeType);

    assertEquals(blockSize, options.getBlockSizeBytes());
    assertEquals(locationPolicy, options.getLocationPolicy());
    assertEquals(owner, options.getOwner());
    assertEquals(group, options.getGroup());
    assertEquals(mode, options.getMode());
    assertEquals(ttl, options.getCommonOptions().getTtl());
    assertEquals(ttlAction, options.getCommonOptions().getTtlAction());
    assertEquals(writeTier, options.getWriteTier());
    assertEquals(writeType.getAlluxioStorageType(), options.getAlluxioStorageType());
    assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
  }

  @Test
  public void equalsTest() {
    ClientContext clientContext = ClientContext.create(mConf);
    new EqualsTester()
        .addEqualityGroup(
            OutStreamOptions.defaults(FileSystemContext.create(clientContext)),
            OutStreamOptions.defaults(FileSystemContext.create(clientContext)))
        .testEquals();
  }
}
