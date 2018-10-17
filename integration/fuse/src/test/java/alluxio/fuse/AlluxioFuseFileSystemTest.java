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

package alluxio.fuse;

import static jnr.constants.platform.OpenFlags.O_RDONLY;
import static jnr.constants.platform.OpenFlags.O_WRONLY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.security.authorization.Mode;
import alluxio.wire.FileInfo;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.util.Collections;
import java.util.List;

/**
 * Isolation tests for {@link AlluxioFuseFileSystem}.
 */
public class AlluxioFuseFileSystemTest {

  private static final String TEST_ROOT_PATH = "/t/root";
  private static final AlluxioURI BASE_EXPECTED_URI = new AlluxioURI(TEST_ROOT_PATH);

  private AlluxioFuseFileSystem mFuseFs;
  private FileSystem mFileSystem;
  private FuseFileInfo mFileInfo;

  @Rule
  public ConfigurationRule mConfiguration =
      new ConfigurationRule(ImmutableMap.of(PropertyKey.FUSE_CACHED_PATHS_MAX, "0",
          PropertyKey.FUSE_SHELL_USER_GROUP_TRANSLATION_ENABLED, "true"));

  @Before
  public void before() throws Exception {
    final List<String> empty = Collections.emptyList();
    AlluxioFuseOptions opts =
        new AlluxioFuseOptions("/doesnt/matter", TEST_ROOT_PATH, false, empty);

    mFileSystem = mock(FileSystem.class);
    try {
      mFuseFs = new AlluxioFuseFileSystem(mFileSystem, opts);
    } catch (UnsatisfiedLinkError e) {
      // stop test and ignore if FuseFileSystem fails to create due to missing libfuse library
      Assume.assumeNoException(e);
    }
    mFileInfo = allocateNativeFileInfo();
  }

  @Test
  public void chmod() throws Exception {
    long mode = 123;
    mFuseFs.chmod("/foo/bar", mode);
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    SetAttributeOptions options = SetAttributeOptions.defaults().setMode(new Mode((short) mode));
    verify(mFileSystem).setAttribute(expectedPath, options);
  }

  @Test
  public void chown() throws Exception {
    long uid = AlluxioFuseUtils.getUid(System.getProperty("user.name"));
    long gid = AlluxioFuseUtils.getGid(System.getProperty("user.name"));
    mFuseFs.chown("/foo/bar", uid, gid);
    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName, gid);
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setGroup(groupName).setOwner(userName);
    verify(mFileSystem).setAttribute(expectedPath, options);
  }

  @Test
  public void create() throws Exception {
    mFileInfo.flags.set(O_WRONLY.intValue());
    mFuseFs.create("/foo/bar", 0, mFileInfo);
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    verify(mFileSystem).createFile(expectedPath);
  }

  @Test
  public void flush() throws Exception {
    FileOutStream fos = mock(FileOutStream.class);
    AlluxioURI anyURI = any();
    when(mFileSystem.createFile(anyURI)).thenReturn(fos);

    // open a file
    mFileInfo.flags.set(O_WRONLY.intValue());
    mFuseFs.create("/foo/bar", 0, mFileInfo);

    // then call flush into it
    mFuseFs.flush("/foo/bar", mFileInfo);
    verify(fos).flush();
  }

  @Test
  public void getattr() throws Exception {
    // set up status
    FileInfo info = new FileInfo();
    info.setLength(10);
    info.setLastModificationTimeMs(1000);
    String userName = System.getProperty("user.name");
    info.setOwner(userName);
    info.setGroup(AlluxioFuseUtils.getGroupName(userName));
    info.setFolder(true);
    info.setMode(123);
    URIStatus status = new URIStatus(info);

    // mock fs
    when(mFileSystem.exists(any(AlluxioURI.class))).thenReturn(true);
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);

    FileStat stat = new FileStat(Runtime.getSystemRuntime());
    assertEquals(0, mFuseFs.getattr("/foo", stat));
    assertEquals(status.getLength(), stat.st_size.longValue());
    assertEquals(status.getLastModificationTimeMs() / 1000, stat.st_ctim.tv_sec.get());
    assertEquals((status.getLastModificationTimeMs() % 1000) * 1000,
        stat.st_ctim.tv_nsec.longValue());
    assertEquals(status.getLastModificationTimeMs() / 1000, stat.st_mtim.tv_sec.get());
    assertEquals((status.getLastModificationTimeMs() % 1000) * 1000,
        stat.st_mtim.tv_nsec.longValue());
    assertEquals(AlluxioFuseUtils.getUid(System.getProperty("user.name")), stat.st_uid.get());
    assertEquals(AlluxioFuseUtils.getGid(System.getProperty("user.name")), stat.st_gid.get());
    assertEquals(123 | FileStat.S_IFDIR, stat.st_mode.intValue());
  }

  @Test
  public void mkDir() throws Exception {
    mFuseFs.mkdir("/foo/bar", -1);
    verify(mFileSystem).createDirectory(BASE_EXPECTED_URI.join("/foo/bar"));
  }

  @Test
  public void openWithoutDelay() throws Exception {
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    setUpOpenMock(expectedPath);

    mFuseFs.open("/foo/bar", mFileInfo);
    verify(mFileSystem).exists(expectedPath);
    verify(mFileSystem).getStatus(expectedPath);
    verify(mFileSystem).openFile(expectedPath);
  }

  @Test
  public void incompleteFileCannotOpen() throws Exception {
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    FileInfo fi = setUpOpenMock(expectedPath);
    fi.setCompleted(false);

    mFuseFs.open("/foo/bar", mFileInfo);
    verify(mFileSystem).exists(expectedPath);
    verify(mFileSystem, atLeast(100)).getStatus(expectedPath);
    verify(mFileSystem, never()).openFile(expectedPath);
  }

  @Test
  public void openWithDelay() throws Exception {
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    FileInfo fi = setUpOpenMock(expectedPath);
    fi.setCompleted(false);

    // Use another thread to open file so that
    // we could change the file status when opening it
    Thread t = new Thread(() -> mFuseFs.open("/foo/bar", mFileInfo));
    t.start();
    Thread.sleep(1000);
    // If the file exists but is not completed, we will wait for the file to complete
    verify(mFileSystem).exists(expectedPath);
    verify(mFileSystem, atLeast(10)).getStatus(expectedPath);
    verify(mFileSystem, never()).openFile(expectedPath);

    fi.setCompleted(true);
    t.join();
    verify(mFileSystem).openFile(expectedPath);
  }

  @Test
  public void read() throws Exception {
    // mocks set-up
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    setUpOpenMock(expectedPath);

    FileInStream fakeInStream = mock(FileInStream.class);
    when(fakeInStream.read(any(byte[].class), anyInt(), anyInt())).then(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
        byte[] myDest = (byte[]) invocationOnMock.getArguments()[0];
        for (byte i = 0; i < 4; i++) {
          myDest[i] = i;
        }
        return 4;
      }
    });
    when(mFileSystem.openFile(expectedPath)).thenReturn(fakeInStream);
    mFileInfo.flags.set(O_RDONLY.intValue());

    // prepare something to read to it
    Runtime r = Runtime.getSystemRuntime();
    Pointer ptr = r.getMemoryManager().allocateTemporary(4, true);

    // actual test
    mFuseFs.open("/foo/bar", mFileInfo);

    mFuseFs.read("/foo/bar", ptr, 4, 0, mFileInfo);
    final byte[] dst = new byte[4];
    ptr.get(0, dst, 0, 4);
    final byte[] expected = new byte[] {0, 1, 2, 3};

    assertArrayEquals("Source and dst data should be equal", expected, dst);
  }

  @Test
  public void rename() throws Exception {
    AlluxioURI oldPath = BASE_EXPECTED_URI.join("/old");
    AlluxioURI newPath = BASE_EXPECTED_URI.join("/new");
    when(mFileSystem.exists(oldPath)).thenReturn(true);
    when(mFileSystem.exists(newPath)).thenReturn(false);
    mFuseFs.rename("/old", "/new");
    verify(mFileSystem).rename(oldPath, newPath);
  }

  @Test
  public void renameOldNotExist() throws Exception {
    AlluxioURI oldPath = BASE_EXPECTED_URI.join("/old");
    when(mFileSystem.exists(oldPath)).thenReturn(false);
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.rename("/old", "/new"));
  }

  @Test
  public void renameNewExist() throws Exception {
    AlluxioURI oldPath = BASE_EXPECTED_URI.join("/old");
    AlluxioURI newPath = BASE_EXPECTED_URI.join("/new");
    when(mFileSystem.exists(oldPath)).thenReturn(true);
    when(mFileSystem.exists(newPath)).thenReturn(true);
    mFuseFs.rename("/old", "/new");
    assertEquals(-ErrorCodes.EEXIST(), mFuseFs.rename("/old", "/new"));
  }

  @Test
  public void write() throws Exception {
    FileOutStream fos = mock(FileOutStream.class);
    AlluxioURI anyURI = any();
    when(mFileSystem.createFile(anyURI)).thenReturn(fos);

    // open a file
    mFileInfo.flags.set(O_WRONLY.intValue());
    mFuseFs.create("/foo/bar", 0, mFileInfo);

    // prepare something to write into it
    Runtime r = Runtime.getSystemRuntime();
    Pointer ptr = r.getMemoryManager().allocateTemporary(4, true);
    byte[] expected = {42, -128, 1, 3};
    ptr.put(0, expected, 0, 4);

    mFuseFs.write("/foo/bar", ptr, 4, 0, mFileInfo);
    verify(fos).write(expected);

    // the second write is no-op because the writes must be sequential and overwriting is supported
    mFuseFs.write("/foo/bar", ptr, 4, 0, mFileInfo);
    verify(fos, times(1)).write(expected);
  }

  @Test
  public void pathTranslation() throws Exception {
    final LoadingCache<String, AlluxioURI> resolver = mFuseFs.getPathResolverCache();

    AlluxioURI expected = new AlluxioURI(TEST_ROOT_PATH);
    AlluxioURI actual = resolver.apply("/");
    assertEquals("/ should resolve to " + expected, expected, actual);

    expected = new AlluxioURI(TEST_ROOT_PATH + "/home/foo");
    actual = resolver.apply("/home/foo");
    assertEquals("/home/foo should resolve to " + expected, expected, actual);
  }

  // Allocate native memory for a FuseFileInfo data struct and return its pointer
  private FuseFileInfo allocateNativeFileInfo() {
    final Runtime runtime = Runtime.getSystemRuntime();
    final Pointer pt = runtime.getMemoryManager().allocateTemporary(36, true);
    return FuseFileInfo.of(pt);
  }

  /**
   * Sets up mock for open() operation.
   *
   * @param uri the path to run operations on
   * @return the file information
   */
  private FileInfo setUpOpenMock(AlluxioURI uri) throws Exception {
    FileInfo fi = new FileInfo();
    fi.setCompleted(true);
    fi.setFolder(false);
    URIStatus status = new URIStatus(fi);

    when(mFileSystem.exists(uri)).thenReturn(true);
    when(mFileSystem.getStatus(uri)).thenReturn(status);
    return fi;
  }
}
