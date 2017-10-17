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
import static jnr.constants.platform.OpenFlags.O_RDWR;
import static jnr.constants.platform.OpenFlags.O_WRONLY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.util.Collections;
import java.util.List;

/**
 * Isolation tests for {@link AlluxioFuseFileSystem}.
 */
// TODO(andreareale): this test suite should be completed
public class AlluxioFuseFileSystemTest {

  private static final String TEST_ROOT_PATH = "/t/root";
  private static final AlluxioURI BASE_EXPECTED_URI = new AlluxioURI(TEST_ROOT_PATH);

  private AlluxioFuseFileSystem mFuseFs;
  private FileSystem mFileSystem;
  private FuseFileInfo mFileInfo;

  @Rule
  public ConfigurationRule mConfiguration =
      new ConfigurationRule(ImmutableMap.of(PropertyKey.FUSE_CACHED_PATHS_MAX, "0"));

  @Before
  public void before() throws Exception {
    final List<String> empty = Collections.emptyList();
    AlluxioFuseOptions opts =
        new AlluxioFuseOptions("/doesnt/matter", TEST_ROOT_PATH, false, empty);

    mFileSystem = mock(FileSystem.class);
    mFuseFs = new AlluxioFuseFileSystem(mFileSystem, opts);
    mFileInfo = allocateNativeFileInfo();
  }

  @Test
  public void create() throws Exception {
    mFileInfo.flags.set(O_WRONLY.intValue());
    mFuseFs.create("/foo/bar", 0, mFileInfo);
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    verify(mFileSystem).createFile(expectedPath);
  }

  @Test
  public void createWrongFlags() throws Exception {
    mFileInfo.flags.set(O_RDONLY.intValue());
    int ret = mFuseFs.create("/foo/bar", 0, mFileInfo);
    verifyZeroInteractions(mFileSystem);
    assertEquals("Expected invalid access", -ErrorCodes.EACCES(), ret);

    mFileInfo.flags.set(O_RDWR.intValue());
    ret = mFuseFs.create("/foo/bar", 0, mFileInfo);
    verifyZeroInteractions(mFileSystem);
    assertEquals("Expected invalid access", -ErrorCodes.EACCES(), ret);
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
  public void mkDir() throws Exception {
    mFuseFs.mkdir("/foo/bar", -1);
    verify(mFileSystem).createDirectory(BASE_EXPECTED_URI.join("/foo/bar"));
  }

  @Test
  public void open() throws Exception {
    // mocks set-up
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    FileInfo fi = new FileInfo();
    fi.setFolder(false);
    URIStatus status = new URIStatus(fi);

    when(mFileSystem.exists(expectedPath)).thenReturn(true);
    when(mFileSystem.getStatus(expectedPath)).thenReturn(status);
    mFileInfo.flags.set(O_RDONLY.intValue());

    // actual test
    mFuseFs.open("/foo/bar", mFileInfo);
    verify(mFileSystem).exists(expectedPath);
    verify(mFileSystem).openFile(expectedPath);
  }

  @Test
  public void openWrongFlags() throws Exception {
    mFileInfo.flags.set(O_RDWR.intValue());

    // actual test
    int ret = mFuseFs.open("/foo/bar", mFileInfo);
    verifyZeroInteractions(mFileSystem);
    assertEquals("Should return an access error", -ErrorCodes.EACCES(), ret);

    mFileInfo.flags.set(O_WRONLY.intValue());
    ret = mFuseFs.open("/foo/bar", mFileInfo);
    verifyZeroInteractions(mFileSystem);
    assertEquals("Should return an access error", -ErrorCodes.EACCES(), ret);
  }

  @Test
  public void read() throws Exception {
    // mocks set-up
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    FileInfo fi = new FileInfo();
    fi.setFolder(false);
    URIStatus status = new URIStatus(fi);

    when(mFileSystem.exists(expectedPath)).thenReturn(true);
    when(mFileSystem.getStatus(expectedPath)).thenReturn(status);

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
}
