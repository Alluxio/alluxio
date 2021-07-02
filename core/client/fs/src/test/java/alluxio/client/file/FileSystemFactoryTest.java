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

package alluxio.client.file;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.SystemPropertyRule;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient;
import alluxio.security.User;
import alluxio.uri.MultiMasterAuthority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

public class FileSystemFactoryTest {
  @Before
  public void before() {
    ConfigurationUtils.reloadProperties();
  }

  @After
  public void after() {
    FileSystem.Factory.FILESYSTEM_CACHE.purge();
  }

  @Test
  public void testCloseRemovesFromCache() throws Exception {
    FileSystem fs1 = FileSystem.Factory.get();
    fs1.close();
    assertTrue("FileSystem should be marked as closed", fs1.isClosed());
    FileSystem fs2 = FileSystem.Factory.get();
    assertFalse("FileSystem shouldn't be closed", fs2.isClosed());
    assertNotSame("Should have different references", fs1, fs2);
  }

  @Test
  public void singleMasterFileSystemCacheTest()  {
    fileSystemCacheTest();
  }

  @Test
  public void multiMasterFileSystemCacheTest()  {
    try (Closeable p = new SystemPropertyRule(PropertyKey.MASTER_RPC_ADDRESSES.getName(),
        "192.168.0.1:1234,192.168.0.2:1445,192.168.0.3:9943").toResource()) {
      ConfigurationUtils.reloadProperties();
      InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
      MasterInquireClient.ConnectDetails connectDetails =
          MasterInquireClient.Factory.getConnectDetails(conf);
      // Make sure we have a MultiMaster authority
      assertTrue(connectDetails.toAuthority() instanceof MultiMasterAuthority);
      fileSystemCacheTest();
    } catch (IOException e) {
      fail("Unable to set system properties");
    }
  }

  @Test
  public void zkFileSystemCacheTest()  {
    Map<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), Boolean.toString(true));
    sysProps.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), "zk@192.168.0.5");
    sysProps.put(PropertyKey.ZOOKEEPER_ELECTION_PATH.getName(), "/alluxio/leader");

    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      ConfigurationUtils.reloadProperties();
      InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
      MasterInquireClient.ConnectDetails connectDetails =
          MasterInquireClient.Factory.getConnectDetails(conf);
      // Make sure we have a Zookeeper authority
      assertTrue(connectDetails.toAuthority() instanceof ZookeeperAuthority);
      fileSystemCacheTest();
    } catch (IOException e) {
      fail("Unable to set system properties");
    }
  }

  @Test
  public void nullSubjectTest()  {
    assertThrows(NullPointerException.class, () -> FileSystem.Factory.get(null));
  }

  @Test
  public void uncachedFileSystemDoesntAffectCache() throws Exception {
    FileSystem fs1 = FileSystem.Factory.get();
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL, "1sec");
    FileSystem fs2 = FileSystem.Factory.create(conf);
    fs2.close();
    FileSystem fs3 = FileSystem.Factory.get();
    assertSame("closing custom config should result in same FileSystem",
        getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs3));
    assertFalse("FileSystem should not be closed", fs1.isClosed());
  }

  // Helper method to get the underlying delegated file system from cache
  private FileSystem getDelegatedFileSystem(FileSystem fs) {
    return ((FileSystemCache.InstanceCachingFileSystem) fs).mDelegatedFileSystem;
  }

  private void fileSystemCacheTest()  {
    FileSystem.Factory.FILESYSTEM_CACHE.purge();
    FileSystem fs1 = FileSystem.Factory.get();
    FileSystem fs2 = FileSystem.Factory.get();
    assertSame("Second client should have been retrieved from cache.",
        getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    fs2 = FileSystem.Factory.get(new Subject());
    assertSame("Passing empty subject should have given the same cached client",
        getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    fs2 = FileSystem.Factory.get(createTestSubject("alluxio-test"));
    assertNotSame("Passing filled subject should have given a new client",
        getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    fs1 = FileSystem.Factory.get(createTestSubject("alluxio-test"));
    assertSame("Second subject with same credentials should return cached client",
        getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    fs2 = FileSystem.Factory.get(createTestSubject("alluxio-test-2"));
    assertNotSame("Passing filled subject should have given a new client",
        getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
  }

  private Subject createTestSubject(String username) {
    User user = new User(username);
    Set<Principal> principals = new HashSet<>();
    principals.add(user);
    return new Subject(false, principals, new HashSet<>(), new HashSet<>());
  }
}
