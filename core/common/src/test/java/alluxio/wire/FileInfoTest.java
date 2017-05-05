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

package alluxio.wire;

import alluxio.util.CommonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileInfoTest {

  @Test
  public void json() throws Exception {
    FileInfo fileInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    FileInfo other = mapper.readValue(mapper.writeValueAsBytes(fileInfo), FileInfo.class);
    checkEquality(fileInfo, other);
  }

  @Test
  public void thrift() {
    FileInfo fileInfo = createRandom();
    FileInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(fileInfo));
    checkEquality(fileInfo, other);
  }

  public void checkEquality(FileInfo a, FileInfo b) {
    Assert.assertEquals(a.getBlockIds(), b.getBlockIds());
    Assert.assertEquals(a.getBlockSizeBytes(), b.getBlockSizeBytes());
    Assert.assertEquals(a.getCreationTimeMs(), b.getCreationTimeMs());
    Assert.assertEquals(a.getFileBlockInfos(), b.getFileBlockInfos());
    Assert.assertEquals(a.getFileId(), b.getFileId());
    Assert.assertEquals(a.getGroup(), b.getGroup());
    Assert.assertEquals(a.getInMemoryPercentage(), b.getInMemoryPercentage());
    Assert.assertEquals(a.getLastModificationTimeMs(), b.getLastModificationTimeMs());
    Assert.assertEquals(a.getLength(), b.getLength());
    Assert.assertEquals(a.getMode(), b.getMode());
    Assert.assertEquals(a.getName(), b.getName());
    Assert.assertEquals(a.getOwner(), b.getOwner());
    Assert.assertEquals(a.getPath(), b.getPath());
    Assert.assertEquals(a.getPersistenceState(), b.getPersistenceState());
    Assert.assertEquals(a.getTtl(), b.getTtl());
    Assert.assertEquals(a.getTtlAction(), b.getTtlAction());
    Assert.assertEquals(a.getMountId(), b.getMountId());
    Assert.assertEquals(a.getUfsPath(), b.getUfsPath());
    Assert.assertEquals(a.getUfsPath(), b.getUfsPath());
    Assert.assertEquals(a.isCacheable(), b.isCacheable());
    Assert.assertEquals(a.isCompleted(), b.isCompleted());
    Assert.assertEquals(a.isFolder(), b.isFolder());
    Assert.assertEquals(a.isMountPoint(), b.isMountPoint());
    Assert.assertEquals(a.isPersisted(), b.isPersisted());
    Assert.assertEquals(a.isPinned(), b.isPinned());
    Assert.assertEquals(a, b);
  }

  public static FileInfo createRandom() {
    FileInfo result = new FileInfo();
    Random random = new Random();

    long fileId = random.nextLong();
    String name = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String path = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String ufsPath = CommonUtils.randomAlphaNumString(random.nextInt(10));
    long mountId = random.nextLong();
    long length = random.nextLong();
    long blockSizeBytes = random.nextLong();
    long creationTimeMs = random.nextLong();
    boolean completed = random.nextBoolean();
    boolean folder = random.nextBoolean();
    boolean pinned = random.nextBoolean();
    boolean cacheable = random.nextBoolean();
    boolean persisted = random.nextBoolean();
    List<Long> blockIds = new ArrayList<>();
    long numBlockIds = random.nextInt(10);
    for (int i = 0; i < numBlockIds; i++) {
      blockIds.add(random.nextLong());
    }
    int inMemoryPercentage = random.nextInt();
    long lastModificationTimeMs = random.nextLong();
    long ttl = random.nextLong();
    String userName = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String groupName = CommonUtils.randomAlphaNumString(random.nextInt(10));
    int permission = random.nextInt();
    String persistenceState = CommonUtils.randomAlphaNumString(random.nextInt(10));
    boolean mountPoint = random.nextBoolean();
    List<FileBlockInfo> fileBlocksInfos = new ArrayList<>();
    long numFileBlockInfos = random.nextInt(10);
    for (int i = 0; i < numFileBlockInfos; i++) {
      fileBlocksInfos.add(FileBlockInfoTest.createRandom());
    }

    result.setBlockIds(blockIds);
    result.setBlockSizeBytes(blockSizeBytes);
    result.setCacheable(cacheable);
    result.setCompleted(completed);
    result.setCreationTimeMs(creationTimeMs);
    result.setFileBlockInfos(fileBlocksInfos);
    result.setFileId(fileId);
    result.setFolder(folder);
    result.setGroup(groupName);
    result.setInMemoryPercentage(inMemoryPercentage);
    result.setLastModificationTimeMs(lastModificationTimeMs);
    result.setLength(length);
    result.setMode(permission);
    result.setMountPoint(mountPoint);
    result.setName(name);
    result.setOwner(userName);
    result.setPath(path);
    result.setPersisted(persisted);
    result.setPersistenceState(persistenceState);
    result.setPinned(pinned);
    result.setTtl(ttl);
    result.setTtlAction(TtlAction.DELETE);
    result.setMountId(mountId);
    result.setUfsPath(ufsPath);
    return result;
  }
}
