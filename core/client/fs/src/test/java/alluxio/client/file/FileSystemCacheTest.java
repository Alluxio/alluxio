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

import static alluxio.client.file.FileSystemCache.Key;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.security.User;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

public class FileSystemCacheTest {
  private FileSystemCache mFileSystemCache = new FileSystemCache();

  // Helper method to get the underlying delegated file system from cache
  private FileSystem getDelegatedFileSystem(FileSystem fs) {
    return ((FileSystemCache.InstanceCachingFileSystem) fs).mDelegatedFileSystem;
  }

  @Test
  public void getSameKey() {
    Key key1 = createTestFSKey("user1");
    FileSystem fs1 = mFileSystemCache.get(key1);
    FileSystem fs2 = mFileSystemCache.get(key1);
    assertSame(getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    assertFalse(fs1.isClosed());
    assertFalse(fs2.isClosed());
  }

  @Test
  public void getDifferentKeys() {
    Key key1 = createTestFSKey("user1");
    Key key2 = createTestFSKey("user2");
    FileSystem fs1 = mFileSystemCache.get(key1);
    FileSystem fs2 = mFileSystemCache.get(key2);
    assertNotSame(getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    assertFalse(fs1.isClosed());
    assertFalse(fs2.isClosed());
  }

  @Test
  public void getThenClose() throws IOException {
    Key key1 = createTestFSKey("user1");
    FileSystem fs1 = mFileSystemCache.get(key1);
    fs1.close();
    FileSystem fs2 = mFileSystemCache.get(key1);
    assertNotSame(getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    assertTrue(fs1.isClosed());
    assertFalse(fs2.isClosed());
  }

  @Test
  public void getTwiceThenClose() throws IOException {
    Key key1 = createTestFSKey("user1");
    FileSystem fs1 = mFileSystemCache.get(key1);
    FileSystem fs2 = mFileSystemCache.get(key1);
    fs1.close();
    FileSystem fs3 = mFileSystemCache.get(key1);
    assertSame(getDelegatedFileSystem(fs2), getDelegatedFileSystem(fs3));
    assertTrue(fs1.isClosed());
    assertFalse(fs2.isClosed());
    assertFalse(fs3.isClosed());
  }

  @Test
  public void getTwiceThenClose2() throws IOException {
    Key key1 = createTestFSKey("user1");
    FileSystem fs1 = mFileSystemCache.get(key1);
    FileSystem fs2 = mFileSystemCache.get(key1);
    assertSame(getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    fs1.close();
    assertTrue(fs1.isClosed());
    assertFalse(fs2.isClosed());
    fs2.close();
    assertTrue(fs1.isClosed());
    assertTrue(fs2.isClosed());
  }

  @Test
  public void doubleClose() throws IOException {
    Key key1 = createTestFSKey("user1");
    FileSystem fs1 = mFileSystemCache.get(key1);
    FileSystem fs2 = mFileSystemCache.get(key1);
    assertSame(getDelegatedFileSystem(fs1), getDelegatedFileSystem(fs2));
    fs1.close();
    assertTrue(fs1.isClosed());
    assertFalse(fs2.isClosed());
    fs1.close();
    assertTrue(fs1.isClosed());
    assertFalse(fs2.isClosed());
  }

  @Test
  public void listStatusClosed() throws IOException, AlluxioException {
    Exception e = assertThrows(IOException.class, () -> {
      Key key1 = createTestFSKey("user1");
      FileSystem fs1 = mFileSystemCache.get(key1);
      fs1.close();
      assertTrue(fs1.isClosed());
      fs1.listStatus(new AlluxioURI("/"));
    });
    assertTrue(e.getMessage()
        .contains(FileSystemCache.InstanceCachingFileSystem.CLOSED_FS_ERROR_MESSAGE));
  }

  private Key createTestFSKey(String username) {
    User user = new User(username);
    Set<Principal> principals = new HashSet<>();
    principals.add(user);
    return new FileSystemCache.Key(
        new Subject(false, principals, new HashSet<>(), new HashSet<>()),
        new InstancedConfiguration(ConfigurationUtils.defaults())
        );
  }
}
