/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import alluxio.util.CommonUtils;

import com.google.common.collect.Lists;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class FileInfoTest {

  @Test
  public void jsonTest() throws Exception {
    FileInfo fileInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    FileInfo other = mapper.readValue(mapper.writeValueAsBytes(fileInfo), FileInfo.class);
    checkEquality(fileInfo, other);
  }

  @Test
  public void thriftTest() {
    FileInfo fileInfo = createRandom();
    FileInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(fileInfo));
    checkEquality(fileInfo, other);
  }

  public void checkEquality(FileInfo a, FileInfo b) {
    Assert.assertEquals(a.getFileId(), b.getFileId());
    Assert.assertEquals(a.getName(), b.getName());
    Assert.assertEquals(a.getPath(), b.getPath());
    Assert.assertEquals(a.getUfsPath(), b.getUfsPath());
    Assert.assertEquals(a.getLength(), b.getLength());
    Assert.assertEquals(a.getBlockSizeBytes(), b.getBlockSizeBytes());
    Assert.assertEquals(a.getCreationTimeMs(), b.getCreationTimeMs());
    Assert.assertEquals(a.isCompleted(), b.isCompleted());
    Assert.assertEquals(a.isFolder(), b.isFolder());
    Assert.assertEquals(a.isPinned(), b.isPinned());
    Assert.assertEquals(a.isCacheable(), b.isCacheable());
    Assert.assertEquals(a.isPersisted(), b.isPersisted());
    Assert.assertEquals(a.getBlockIds(), b.getBlockIds());
    Assert.assertEquals(a.getInMemoryPercentage(), b.getInMemoryPercentage());
    Assert.assertEquals(a.getLastModificationTimeMs(), b.getLastModificationTimeMs());
    Assert.assertEquals(a.getTtl(), b.getTtl());
    Assert.assertEquals(a.getUserName(), b.getUserName());
    Assert.assertEquals(a.getGroupName(), b.getGroupName());
    Assert.assertEquals(a.getPermission(), b.getPermission());
    Assert.assertEquals(a.getPersistenceState(), b.getPersistenceState());
    Assert.assertEquals(a.isMountPoint(), b.isMountPoint());
    Assert.assertEquals(a, b);
  }

  public static FileInfo createRandom() {
    FileInfo result = new FileInfo();
    Random random = new Random();

    long fileId = random.nextLong();
    String name = CommonUtils.randomString(random.nextInt(10));
    String path = CommonUtils.randomString(random.nextInt(10));
    String ufsPath = CommonUtils.randomString(random.nextInt(10));
    long length = random.nextLong();
    long blockSizeBytes = random.nextLong();
    long creationTimeMs = random.nextLong();
    boolean completed = random.nextBoolean();
    boolean folder = random.nextBoolean();
    boolean pinned = random.nextBoolean();
    boolean cacheable = random.nextBoolean();
    boolean persisted = random.nextBoolean();
    List<Long> blockIds = Lists.newArrayList();
    long numBlockIds = random.nextInt(10);
    for (int i = 0; i < numBlockIds; i++) {
      blockIds.add(random.nextLong());
    }
    int inMemoryPercentage = random.nextInt();
    long lastModificationTimeMs = random.nextLong();
    long ttl = random.nextLong();
    String userName = CommonUtils.randomString(random.nextInt(10));
    String groupName = CommonUtils.randomString(random.nextInt(10));
    int permission = random.nextInt();
    String persistenceState = CommonUtils.randomString(random.nextInt(10));
    boolean mountPoint = random.nextBoolean();

    result.setFileId(fileId);
    result.setName(name);
    result.setPath(path);
    result.setUfsPath(ufsPath);
    result.setLength(length);
    result.setBlockSizeBytes(blockSizeBytes);
    result.setCreationTimeMs(creationTimeMs);
    result.setCompleted(completed);
    result.setFolder(folder);
    result.setPinned(pinned);
    result.setCacheable(cacheable);
    result.setPersisted(persisted);
    result.setBlockIds(blockIds);
    result.setInMemoryPercentage(inMemoryPercentage);
    result.setLastModificationTimeMs(lastModificationTimeMs);
    result.setTtl(ttl);
    result.setUserName(userName);
    result.setGroupName(groupName);
    result.setPermission(permission);
    result.setPersistenceState(persistenceState);
    result.setMountPoint(mountPoint);

    return result;
  }
}
