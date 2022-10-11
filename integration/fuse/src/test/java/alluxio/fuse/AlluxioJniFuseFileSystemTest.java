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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.Mode;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.FileInfo;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;

/**
 * Isolation tests for {@link AlluxioJniFuseFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.Factory.class})
public class AlluxioJniFuseFileSystemTest {

  private static final String TEST_ROOT_PATH = "/t/root";
  private static final AlluxioURI BASE_EXPECTED_URI = new AlluxioURI(TEST_ROOT_PATH);
  private static final String MOUNT_POINT = "/t/mountPoint";

  private final InstancedConfiguration mConf = Configuration.copyGlobal();

  private AlluxioJniFuseFileSystem mFuseFs;
  private FileSystemContext mFileSystemContext;
  private FileSystem mFileSystem;
  private FuseFileInfo mFileInfo;

  @Rule
  public ConfigurationRule mConfiguration =
      new ConfigurationRule(ImmutableMap.of(PropertyKey.FUSE_CACHED_PATHS_MAX, 0,
          PropertyKey.FUSE_MOUNT_ALLUXIO_PATH, TEST_ROOT_PATH,
          PropertyKey.FUSE_MOUNT_POINT, MOUNT_POINT), mConf);

  @Before
  public void before() throws Exception {
    mFileSystemContext = mock(FileSystemContext.class);
    mFileSystem = mock(FileSystem.class);
    when(mFileSystemContext.getClusterConf()).thenReturn(mConf);
    try {
      mFuseFs = new AlluxioJniFuseFileSystem(
          mFileSystemContext, mFileSystem);
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
    SetAttributePOptions options =
        SetAttributePOptions.newBuilder().setMode(new Mode((short) mode).toProto()).build();
    verify(mFileSystem).setAttribute(expectedPath, options);
  }

  @Test
  public void chown() throws Exception {
    Optional<Long> uid = AlluxioFuseUtils.getUid(System.getProperty("user.name"));
    // avoid using the launch user
    if (uid.isPresent() && uid.get().equals(AlluxioFuseUtils.getSystemUid())) {
      uid = Optional.of(uid.get() + 1);
    }
    assertTrue(uid.isPresent());
    Optional<String> userName = AlluxioFuseUtils.getUserName(uid.get());
    if (!userName.isPresent()) {
      // skip this case for such an environment
      return;
    }
    Optional<Long> gid = AlluxioFuseUtils.getGidFromUserName(userName.get());
    assertTrue(gid.isPresent());
    URIStatus status = mock(URIStatus.class);
    when(status.getOwner()).thenReturn("user");
    when(status.getGroup()).thenReturn("group");
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);
    mFuseFs.chown("/foo/bar", uid.get(), gid.get());
    Optional<String> groupName = AlluxioFuseUtils.getGroupName(gid.get());
    assertTrue(groupName.isPresent());
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    SetAttributePOptions options =
        SetAttributePOptions.newBuilder().setGroup(groupName.get())
            .setOwner(userName.get()).build();
    verify(mFileSystem).setAttribute(expectedPath, options);
  }

  @Test
  public void chownWithoutValidGid() throws Exception {
    Optional<Long> uid = AlluxioFuseUtils.getUid(System.getProperty("user.name"));
    assertTrue(uid.isPresent());
    long gid = AlluxioFuseUtils.ID_NOT_SET_VALUE;
    URIStatus status = mock(URIStatus.class);
    when(status.getOwner()).thenReturn("user");
    when(status.getGroup()).thenReturn("group");
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);
    mFuseFs.chown("/foo/bar", uid.get(), gid);
    String userName = System.getProperty("user.name");
    Optional<String> groupName = AlluxioFuseUtils.getGroupName(userName);
    assertTrue(groupName.isPresent());
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    // invalid gid will not be contained in options
    SetAttributePOptions options =
        SetAttributePOptions.newBuilder().setOwner(userName).build();
    verify(mFileSystem).setAttribute(expectedPath, options);

    gid = AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED;
    mFuseFs.chown("/foo/bar", uid.get(), gid);
    verify(mFileSystem, times(2)).setAttribute(expectedPath, options);
  }

  @Test
  public void chownWithoutValidUid() throws Exception {
    String userName = System.getProperty("user.name");
    long uid = AlluxioFuseUtils.ID_NOT_SET_VALUE;
    Optional<Long> gid = AlluxioFuseUtils.getGidFromUserName(userName);
    assertTrue(gid.isPresent());
    URIStatus status = mock(URIStatus.class);
    when(status.getOwner()).thenReturn("user");
    when(status.getGroup()).thenReturn("group");
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);
    mFuseFs.chown("/foo/bar", uid, gid.get());

    Optional<String> groupName = AlluxioFuseUtils.getGroupName(userName);
    assertTrue(groupName.isPresent());
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    SetAttributePOptions options = SetAttributePOptions.newBuilder()
        .setGroup(groupName.get()).build();
    verify(mFileSystem).setAttribute(expectedPath, options);

    uid = AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED;
    mFuseFs.chown("/foo/bar", uid, gid.get());
    verify(mFileSystem, times(2)).setAttribute(expectedPath, options);
  }

  @Test
  public void chownWithoutValidUidAndGid() throws Exception {
    long uid = AlluxioFuseUtils.ID_NOT_SET_VALUE;
    long gid = AlluxioFuseUtils.ID_NOT_SET_VALUE;
    mFuseFs.chown("/foo/bar", uid, gid);
    verify(mFileSystem, never()).setAttribute(any());

    uid = AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED;
    gid = AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED;
    mFuseFs.chown("/foo/bar", uid, gid);
    verify(mFileSystem, never()).setAttribute(any());
  }

  @Test
  public void create() throws Exception {
    // "create" checks if the file already exists first
    when(mFileSystem.getStatus(any(AlluxioURI.class)))
        .thenThrow(mock(FileDoesNotExistException.class));
    mFileInfo.flags.set(O_WRONLY.intValue());
    mFuseFs.create("/foo/bar", 0, mFileInfo);
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    verify(mFileSystem).createFile(expectedPath, CreateFilePOptions.newBuilder()
        .setMode(new alluxio.security.authorization.Mode((short) 0).toProto())
        .build());
  }

  @Test
  public void createWithLengthLimit() {
    String c256 = String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
    mFileInfo.flags.set(O_WRONLY.intValue());
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.create("/foo/" + c256, 0, mFileInfo));
  }

  @Test
  public void flush() throws Exception {
    FileOutStream fos = mock(FileOutStream.class);
    AlluxioURI anyURI = any();
    CreateFilePOptions options = any();
    when(mFileSystem.createFile(anyURI, options)).thenReturn(fos);
    // "create" checks if the file already exists first
    when(mFileSystem.getStatus(any(AlluxioURI.class)))
        .thenThrow(mock(FileDoesNotExistException.class));

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
    info.setLength(4 * Constants.KB + 1);
    info.setLastAccessTimeMs(1000);
    info.setLastModificationTimeMs(1000);
    String userName = System.getProperty("user.name");
    info.setOwner(userName);
    Optional<String> groupName = AlluxioFuseUtils.getGroupName(userName);
    assertTrue(groupName.isPresent());
    info.setGroup(groupName.get());
    info.setFolder(true);
    info.setMode(123);
    info.setCompleted(true);
    URIStatus status = new URIStatus(info);

    // mock fs
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);

    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    assertEquals(0, mFuseFs.getattr("/foo", stat));
    assertEquals(status.getLength(), stat.st_size.longValue());
    assertEquals(9, stat.st_blocks.intValue());
    assertEquals(status.getLastAccessTimeMs() / 1000, stat.st_atim.tv_sec.get());
    assertEquals((status.getLastAccessTimeMs() % 1000) * 1000,
            stat.st_atim.tv_nsec.longValue());
    assertEquals(status.getLastModificationTimeMs() / 1000, stat.st_ctim.tv_sec.get());
    assertEquals((status.getLastModificationTimeMs() % 1000) * 1000,
        stat.st_ctim.tv_nsec.longValue());
    assertEquals(status.getLastModificationTimeMs() / 1000, stat.st_mtim.tv_sec.get());
    assertEquals((status.getLastModificationTimeMs() % 1000) * 1000,
        stat.st_mtim.tv_nsec.longValue());
    Optional<Long> uid = AlluxioFuseUtils.getUid(System.getProperty("user.name"));
    Optional<Long> gid = AlluxioFuseUtils.getGidFromUserName(System.getProperty("user.name"));
    assertTrue(uid.isPresent());
    assertTrue(gid.isPresent());
    assertEquals((long) uid.get(), stat.st_uid.get());
    assertEquals((long) gid.get(), stat.st_gid.get());
    assertEquals(123 | FileStat.S_IFDIR, stat.st_mode.intValue());
  }

  @Test
  public void getattrWithDelay() throws Exception {
    String path = "/foo/bar";
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");

    // set up status
    FileInfo info = new FileInfo();
    info.setLength(0);
    info.setCompleted(false);
    URIStatus status = new URIStatus(info);

    // mock fs
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);

    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));

    // Use another thread to open file so that
    // we could change the file status when opening it
    Thread t = new Thread(() -> mFuseFs.getattr(path, stat));
    t.start();
    Thread.sleep(1000);

    // If the file is not being written and is not completed,
    // we will wait for the file to complete
    verify(mFileSystem, atLeast(10)).getStatus(expectedPath);
    assertEquals(0, stat.st_size.longValue());

    info.setCompleted(true);
    info.setLength(1000);

    t.join();

    assertEquals(1000, stat.st_size.longValue());
  }

  @Test
  public void getattrWhenWriting() throws Exception {
    String path = "/foo/bar";
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join(path);

    FileOutStream fos = mock(FileOutStream.class);
    when(mFileSystem.createFile(expectedPath)).thenReturn(fos);

    mFuseFs.create(path, 0, mFileInfo);

    // Prepare file status
    FileInfo info = new FileInfo();
    info.setLength(0);
    info.setCompleted(false);
    URIStatus status = new URIStatus(info);

    when(mFileSystem.exists(any(AlluxioURI.class))).thenReturn(true);
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);

    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));

    // getattr() will not be blocked when writing
    mFuseFs.getattr(path, stat);
    // If getattr() is blocking, it will continuously get status of the file
    verify(mFileSystem, atMost(300)).getStatus(expectedPath);
    assertEquals(0, stat.st_size.longValue());

    mFuseFs.release(path, mFileInfo);

    // getattr() will be blocked waiting for the file to be completed
    // If release() is called (returned) but does not finished
    Thread t = new Thread(() -> mFuseFs.getattr(path, stat));
    t.start();
    Thread.sleep(1000);
    verify(mFileSystem, atLeast(10)).getStatus(expectedPath);
    assertEquals(0, stat.st_size.longValue());

    info.setCompleted(true);
    info.setLength(1000);

    t.join();

    // getattr() completed and set the file size
    assertEquals(1000, stat.st_size.longValue());
  }

  @Test
  public void mkDir() throws Exception {
    long mode = 0755L;
    mFuseFs.mkdir("/foo/bar", mode);
    verify(mFileSystem).createDirectory(BASE_EXPECTED_URI.join("/foo/bar"),
        CreateDirectoryPOptions.newBuilder()
            .setMode(new alluxio.security.authorization.Mode((short) mode).toProto())
            .build());
  }

  @Test
  public void mkDirWithLengthLimit() {
    long mode = 0755L;
    String c256 = String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.mkdir("/foo/" + c256, mode));
  }

  @Test
  public void openWithoutDelay() throws Exception {
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    setUpOpenMock(expectedPath);

    FileInStream is = mock(FileInStream.class);
    when(mFileSystem.openFile(expectedPath)).thenReturn(is);
    mFuseFs.open("/foo/bar", mFileInfo);
    verify(mFileSystem).openFile(expectedPath);
  }

  @Test
  public void incompleteFileCannotOpen() throws Exception {
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    FileInfo fi = setUpOpenMock(expectedPath);
    fi.setCompleted(false);

    when(mFileSystem.openFile(expectedPath)).thenThrow(new FileIncompleteException(expectedPath));
    assertEquals(-ErrorCodes.EIO(), mFuseFs.open("/foo/bar", mFileInfo));
  }

  @Test
  public void read() throws Exception {
    // mocks set-up
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    setUpOpenMock(expectedPath);

    FileInStream fakeInStream = mock(FileInStream.class);
    mFileSystem.getStatus(expectedPath).getFileInfo().setLength(4);
    when(fakeInStream.read(any(ByteBuffer.class),
        anyInt(), anyInt())).then((Answer<Integer>) invocationOnMock -> {
          ByteBuffer myDest = (ByteBuffer) invocationOnMock.getArguments()[0];
          for (byte i = 0; i < 4; i++) {
            myDest.put(i, i);
          }
          return 4;
        });

    when(mFileSystem.openFile(expectedPath)).thenReturn(fakeInStream);
    mFileInfo.flags.set(O_RDONLY.intValue());

    // prepare something to read to it
    ByteBuffer ptr = ByteBuffer.allocateDirect(4);
    assertEquals(4, ptr.limit());

    // actual test
    mFuseFs.open("/foo/bar", mFileInfo);

    mFuseFs.read("/foo/bar", ptr, 4, 0, mFileInfo);
    final byte[] dst = new byte[4];
    ptr.get(dst, 0, 4);
    final byte[] expected = new byte[] {0, 1, 2, 3};

    assertArrayEquals("Source and dst data should be equal", expected, dst);
  }

  @Test
  public void rename() throws Exception {
    AlluxioURI oldPath = BASE_EXPECTED_URI.join("/old");
    AlluxioURI newPath = BASE_EXPECTED_URI.join("/new");
    doNothing().when(mFileSystem).rename(oldPath, newPath);
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(mock(URIStatus.class));
    setUpOpenMock(oldPath);
    mFuseFs.rename("/old", "/new", AlluxioJniRenameUtils.NO_FLAGS);
    verify(mFileSystem).rename(oldPath, newPath);
  }

  @Test
  public void renameOldNotExist() throws Exception {
    AlluxioURI oldPath = BASE_EXPECTED_URI.join("/old");
    AlluxioURI newPath = BASE_EXPECTED_URI.join("/new");
    doThrow(new FileDoesNotExistException("File /old does not exist"))
        .when(mFileSystem).rename(oldPath, newPath);
    when(mFileSystem.getStatus(any(AlluxioURI.class)))
        .thenThrow(new FileDoesNotExistException("File /old does not exist"));
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.rename("/old", "/new",
        AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void renameNewExist() throws Exception {
    AlluxioURI oldPath = BASE_EXPECTED_URI.join("/old");
    AlluxioURI newPath = BASE_EXPECTED_URI.join("/new");
    doThrow(new FileAlreadyExistsException("File /new already exists"))
        .when(mFileSystem).rename(oldPath, newPath);
    when(mFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(mock(URIStatus.class));
    setUpOpenMock(oldPath);
    assertEquals(-ErrorCodes.EIO(), mFuseFs.rename("/old", "/new",
        AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void renameWithLengthLimit() throws Exception {
    String c256 = String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
    AlluxioURI oldPath = BASE_EXPECTED_URI.join("/old");
    AlluxioURI newPath = BASE_EXPECTED_URI.join("/" + c256);
    doNothing().when(mFileSystem).rename(oldPath, newPath);
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.rename("/old", "/" + c256, AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void rmdir() throws Exception {
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    doNothing().when(mFileSystem).delete(expectedPath);
    mFuseFs.rmdir("/foo/bar");
    verify(mFileSystem).delete(expectedPath);
  }

  @Test
  public void write() throws Exception {
    FileOutStream fos = mock(FileOutStream.class);
    AlluxioURI anyURI = any();
    CreateFilePOptions options = any();
    when(mFileSystem.createFile(anyURI, options)).thenReturn(fos);
    // "create" checks if the file already exists first
    when(mFileSystem.getStatus(any(AlluxioURI.class)))
        .thenThrow(mock(FileDoesNotExistException.class));

    // open a file
    mFileInfo.flags.set(O_WRONLY.intValue());
    mFuseFs.create("/foo/bar", 0, mFileInfo);

    // prepare something to write into it
    ByteBuffer ptr = ByteBuffer.allocateDirect(4);
    byte[] expected = {42, -128, 1, 3};
    ptr.put(expected, 0, 4);
    ptr.flip();

    mFuseFs.write("/foo/bar", ptr, 4, 0, mFileInfo);
    verify(fos).write(expected);

    // the second write is no-op because the writes must be sequential and overwriting is supported
    mFuseFs.write("/foo/bar", ptr, 4, 0, mFileInfo);
    verify(fos, times(1)).write(expected);
  }

  @Test
  public void unlink() throws Exception {
    AlluxioURI expectedPath = BASE_EXPECTED_URI.join("/foo/bar");
    doNothing().when(mFileSystem).delete(expectedPath);
    mFuseFs.unlink("/foo/bar");
    verify(mFileSystem).delete(expectedPath);
  }

  @Test
  public void pathTranslation() {
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
    ByteBuffer buffer = ByteBuffer.allocateDirect(36);
    buffer.clear();
    return FuseFileInfo.of(buffer);
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

    when(mFileSystem.getStatus(uri)).thenReturn(status);
    return fi;
  }

  @Test
  public void statfs() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4 * Constants.KB);
    buffer.clear();
    Statvfs stbuf = Statvfs.of(buffer);

    int blockSize = 16 * Constants.KB;
    int totalBlocks = 4;
    int freeBlocks = 3;

    BlockMasterClient blockMasterClient = PowerMockito.mock(BlockMasterClient.class);
    PowerMockito.mockStatic(BlockMasterClient.Factory.class);
    when(BlockMasterClient.Factory.create(any())).thenReturn(blockMasterClient);

    BlockMasterInfo blockMasterInfo = new BlockMasterInfo();
    blockMasterInfo.setCapacityBytes(totalBlocks * blockSize);
    blockMasterInfo.setFreeBytes(freeBlocks * blockSize);
    when(blockMasterClient.getBlockMasterInfo(any())).thenReturn(blockMasterInfo);
    when(mFileSystemContext.acquireBlockMasterClientResource()).thenReturn(
        new CloseableResource<BlockMasterClient>(blockMasterClient) {
          @Override
          public void closeResource() {}
        });

    assertEquals(0, mFuseFs.statfs("/", stbuf));

    assertEquals(blockSize, stbuf.f_bsize.intValue());
    assertEquals(blockSize, stbuf.f_frsize.intValue());
    assertEquals(totalBlocks, stbuf.f_blocks.longValue());
    assertEquals(freeBlocks, stbuf.f_bfree.longValue());
    assertEquals(freeBlocks, stbuf.f_bavail.longValue());

    assertEquals(AlluxioJniFuseFileSystem.UNKNOWN_INODES, stbuf.f_files.intValue());
    assertEquals(AlluxioJniFuseFileSystem.UNKNOWN_INODES, stbuf.f_ffree.intValue());
    assertEquals(AlluxioJniFuseFileSystem.UNKNOWN_INODES, stbuf.f_favail.intValue());
    assertEquals(AlluxioFuseUtils.MAX_NAME_LENGTH, stbuf.f_namemax.intValue());
  }
}
