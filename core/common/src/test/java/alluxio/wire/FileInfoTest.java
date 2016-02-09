/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.wire;

import java.util.List;
import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

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
    Assert.assertEquals(a, b);
  }

  public static FileInfo createRandom() {
    FileInfo result = new FileInfo();
    Random random = new Random();

    long fileId = random.nextLong();
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String name = new String(bytes);
    random.nextBytes(bytes);
    String path = new String(bytes);
    random.nextBytes(bytes);
    String ufsPath = new String(bytes);
    long length = random.nextLong();
    long blockSizeBytes = random.nextLong();
    long creationTimeMs = random.nextLong();
    boolean completed = random.nextBoolean();
    boolean folder = random.nextBoolean();
    boolean pinned = random.nextBoolean();
    boolean cacheable = random.nextBoolean();
    boolean persisted = random.nextBoolean();
    List<Long> blockIds = Lists.newArrayList();
    long numElements = random.nextInt(10) + 1;
    for (int i = 0; i < numElements; i++) {
      blockIds.add(random.nextLong());
    }
    int inMemoryPercentage = random.nextInt();
    long lastModificationTimeMs = random.nextLong();
    long ttl = random.nextLong();
    random.nextBytes(bytes);
    String userName = new String(bytes);
    random.nextBytes(bytes);
    String groupName = new String(bytes);
    int permission = random.nextInt();
    random.nextBytes(bytes);
    String persistenceState = new String(bytes);

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

    return result;
  }
}
