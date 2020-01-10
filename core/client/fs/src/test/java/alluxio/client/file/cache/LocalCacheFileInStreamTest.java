package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MockFileInStream;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Unit tests for {@link LocalCacheFileInStream}.
 */
public class LocalCacheFileInStreamTest {
  @Test
  public void readFullPage() throws Exception {
    int fileSize = (int) LocalCacheFileInStream.PAGE_SIZE;
    byte[] testData = generateData(fileSize);
    ByteArrayCacheManager manager = new ByteArrayCacheManager();
    LocalCacheFileInStream stream = setupWithSingleFile(testData, manager);

    // cache miss
    byte[] cacheMiss = new byte[fileSize];
    Assert.assertEquals(fileSize, stream.read(cacheMiss));
    Assert.assertArrayEquals(testData, cacheMiss);
    Assert.assertEquals(0, manager.mPagesServed);
    Assert.assertEquals(1, manager.mPagesCached);

    // cache hit
    stream.seek(0);
    byte[] cacheHit = new byte[fileSize];
    Assert.assertEquals(fileSize, stream.read(cacheHit));
    Assert.assertArrayEquals(testData, cacheHit);
    Assert.assertEquals(1, manager.mPagesServed);
  }

  @Test
  public void readSmallPage() throws Exception {
    int fileSize = (int) LocalCacheFileInStream.PAGE_SIZE / 5;
    byte[] testData = generateData(fileSize);
    ByteArrayCacheManager manager = new ByteArrayCacheManager();
    LocalCacheFileInStream stream = setupWithSingleFile(testData, manager);

    // cache miss
    byte[] cacheMiss = new byte[fileSize];
    Assert.assertEquals(fileSize, stream.read(cacheMiss));
    Assert.assertArrayEquals(testData, cacheMiss);
    Assert.assertEquals(0, manager.mPagesServed);
    Assert.assertEquals(1, manager.mPagesCached);

    // cache hit
    stream.seek(0);
    byte[] cacheHit = new byte[fileSize];
    Assert.assertEquals(fileSize, stream.read(cacheHit));
    Assert.assertArrayEquals(testData, cacheHit);
    Assert.assertEquals(1, manager.mPagesServed);
  }

  @Ignore
  @Test
  // TODO(calvin): this test should pass after we pass in offset in the get page API
  public void readPartialPage() throws Exception {
    int fileSize = (int) LocalCacheFileInStream.PAGE_SIZE;
    byte[] testData = generateData(fileSize);
    ByteArrayCacheManager manager = new ByteArrayCacheManager();
    LocalCacheFileInStream stream = setupWithSingleFile(testData, manager);

    int partialReadSize = fileSize / 5;
    int offset = fileSize / 5;

    // cache miss
    byte[] cacheMiss = new byte[partialReadSize];
    stream.seek(offset);
    Assert.assertEquals(partialReadSize, stream.read(cacheMiss));
    Assert.assertArrayEquals(
        Arrays.copyOfRange(testData, offset, offset + partialReadSize), cacheMiss);
    Assert.assertEquals(0, manager.mPagesServed);
    Assert.assertEquals(1, manager.mPagesCached);

    // cache hit
    byte[] cacheHit = new byte[partialReadSize];
    stream.seek(offset);
    Assert.assertEquals(partialReadSize, stream.read(cacheHit));
    Assert.assertArrayEquals(
        Arrays.copyOfRange(testData, offset, offset + partialReadSize), cacheHit);
    Assert.assertEquals(1, manager.mPagesServed);
  }

  @Test
  public void readMultiPage() throws Exception {
    int pages = 2;
    int fileSize = (int) LocalCacheFileInStream.PAGE_SIZE + 10;
    byte[] testData = generateData(fileSize);
    ByteArrayCacheManager manager = new ByteArrayCacheManager();
    LocalCacheFileInStream stream = setupWithSingleFile(testData, manager);

    // cache miss
    byte[] cacheMiss = new byte[fileSize];
    Assert.assertEquals(fileSize, stream.read(cacheMiss));
    Assert.assertArrayEquals(testData, cacheMiss);
    Assert.assertEquals(0, manager.mPagesServed);
    Assert.assertEquals(pages, manager.mPagesCached);

    // cache hit
    stream.seek(0);
    byte[] cacheHit = new byte[fileSize];
    Assert.assertEquals(fileSize, stream.read(cacheHit));
    Assert.assertArrayEquals(testData, cacheHit);
    Assert.assertEquals(pages, manager.mPagesServed);
  }

  @Test
  public void readMultiPageMixed() throws Exception {
    int pages = 10;
    int fileSize = (int) LocalCacheFileInStream.PAGE_SIZE * pages;
    byte[] testData = generateData(fileSize);
    ByteArrayCacheManager manager = new ByteArrayCacheManager();
    LocalCacheFileInStream stream = setupWithSingleFile(testData, manager);

    // populate cache
    int pagesCached = 0;
    for (int i = 0; i < pages; i++) {
      stream.seek(LocalCacheFileInStream.PAGE_SIZE * i);
      if (ThreadLocalRandom.current().nextBoolean()) {
        Assert.assertEquals(testData[(int) (i * LocalCacheFileInStream.PAGE_SIZE)], stream.read());
        pagesCached++;
      }
    }

    Assert.assertEquals(0, manager.mPagesServed);
    Assert.assertEquals(pagesCached, manager.mPagesCached);

    // sequential read
    stream.seek(0);
    byte[] fullRead = new byte[fileSize];
    Assert.assertEquals(fileSize, stream.read(fullRead));
    Assert.assertArrayEquals(testData, fullRead);
    Assert.assertEquals(pagesCached, manager.mPagesServed);
  }

  private LocalCacheFileInStream setupWithSingleFile(byte[] data, CacheManager manager) {
    Map<AlluxioURI, byte[]> files = new HashMap<>();
    AlluxioURI testFilename = new AlluxioURI("/test");
    files.put(testFilename, data);

    ByteArrayFileSystem fs = new ByteArrayFileSystem(files);

    return new LocalCacheFileInStream(
        testFilename, OpenFilePOptions.getDefaultInstance(), fs, manager);
  }

  private URIStatus generateURIStatus(String path, long len) {
    FileInfo info = new FileInfo();
    info.setPath(path);
    info.setLength(len);
    return new URIStatus(info);
  }

  private byte[] generateData(int len) {
    byte[] data = new byte[len];
    for (int i = 0; i < len; i++) {
      data[i] = (byte) i;
    }
    return data;
  }

  /**
   * Implementation of cache manager that stores cached data in byte arrays in memory.
   */
  private class ByteArrayCacheManager implements CacheManager {
    private final Map<Pair<Long, Long>, byte[]> mPages;

    /** Metrics for test validation. */
    long mPagesServed = 0;
    long mPagesCached = 0;

    ByteArrayCacheManager() {
      mPages = new HashMap<>();
    }

    @Override
    public int put(long fileId, long pageId, byte[] page) throws IOException {
      mPages.put(new Pair<>(fileId, pageId), page);
      mPagesCached++;
      return page.length;
    }

    @Override
    public ReadableByteChannel get(long fileId, long pageId) throws IOException {
      Pair<Long, Long> key = new Pair<>(fileId, pageId);
      if (!mPages.containsKey(key)) {
        return null;
      }
      mPagesServed++;
      return Channels.newChannel(new ByteArrayInputStream(mPages.get(key)));
    }

    @Override
    public boolean delete(long fileId, long pageId) throws IOException {
      mPages.remove(new Pair<>(fileId, pageId));
      return true;
    }
  }

  /**
   * An implementation of file system which is a store of filenames to byte arrays. Only
   * {@link FileSystem#openFile(AlluxioURI)} and its variants are supported.
   */
  private class ByteArrayFileSystem implements FileSystem {
    private final Map<AlluxioURI, byte[]> mFiles;

    ByteArrayFileSystem(Map<AlluxioURI, byte[]> files) {
      mFiles = files;
    }

    @Override
    public boolean isClosed() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
        throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
        throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(AlluxioURI path, DeletePOptions options)
        throws DirectoryNotEmptyException, FileDoesNotExistException, IOException,
        AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(AlluxioURI path, ExistsPOptions options)
        throws InvalidPathException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void free(AlluxioURI path, FreePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public AlluxioConfiguration getConf() {
      throw new UnsupportedOperationException();
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mFiles.containsKey(path)) {
        return generateURIStatus(path.getPath(), mFiles.get(path).length);
      } else {
        throw new FileDoesNotExistException(path);
      }
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, MountPointInfo> getMountTable()
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
        throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
        IOException, AlluxioException {
      if (mFiles.containsKey(path)) {
        return new MockFileInStream(mFiles.get(path));
      } else {
        throw new FileDoesNotExistException(path);
      }
    }

    @Override
    public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public AlluxioURI reverseResolve(AlluxioURI ufsUri)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
        SetAclPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void startSync(AlluxioURI path)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stopSync(AlluxioURI path)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(AlluxioURI path, SetAttributePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unmount(AlluxioURI path, UnmountPOptions options)
        throws IOException, AlluxioException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
