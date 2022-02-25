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

package alluxio.client.file.cache.store;

import alluxio.client.file.cache.FileInfo;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.quota.CacheScope;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.proto.client.file.cache.FileMeta.ProtoFileMeta;
import alluxio.proto.client.file.cache.FileMeta.ProtoFileMeta.Builder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link LocalPageStore} is an implementation of {@link PageStore} which
 * stores all pages in a directory somewhere on the local disk.
 */
@NotThreadSafe
public class LocalPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(LocalPageStore.class);
  private static final String ERROR_NO_SPACE_LEFT = "No space left on device";
  private static final String FILE_METADATA_FILENAME = "metadata";
  /** The depth of the file level directory from root-path. */
  public static final int FILE_LEVEL_DIR_DEPTH = 3;
  private final List<Path> mRoots;
  private final long mPageSize;
  private final long mCapacity;
  private final int mFileBuckets;
  private final Pattern mPagePattern;
  private final Pattern mFilePattern;

  /**
   * Creates a new instance of {@link LocalPageStore}.
   *
   * @param options options for the local page store
   */
  public LocalPageStore(LocalPageStoreOptions options) {
    mRoots = options.getRootDirs();
    mPageSize = options.getPageSize();
    mCapacity = (long) (options.getCacheSize() / (1 + options.getOverheadRatio()));
    mFileBuckets = options.getFileBuckets();
    // pattern encoding for each page
    // root_path/page_size(ulong)/bucket(uint)/file_id(str)/mtime(ulong)/page_idx(ulong)
    mPagePattern = Pattern.compile(
        String.format("%s/%d/(\\d+)/([^/]+)/(\\d+)/(\\d+)", "("
                + mRoots.stream().map(path -> path.toString()).reduce((a, b) -> a + "|" + b).get()
                + ")",
            mPageSize));
    // pattern encoding for the directory of each file
    // root_path/page_size(ulong)/bucket(uint)/file_id(str)/
    mFilePattern = Pattern.compile(
        String.format("%s/%d/(\\d+)/([^/]+)", "("
                + mRoots.stream().map(path -> path.toString()).reduce((a, b) -> a + "|" + b).get()
                + ")",
            mPageSize));
  }

  @Override
  public void put(PageInfo pageInfo, byte[] page)
      throws ResourceExhaustedException, IOException {
    PageId pageId = pageInfo.getPageId();
    Path pagePath = getPageFilePath(pageInfo);
    Path timestampPath = Preconditions
        .checkNotNull(pagePath.getParent(), "timestamp path should not be null");
    try {
      if (!Files.exists(pagePath)) {
        if (!Files.exists(timestampPath)) { //new timestamp, new file or file got updated
          Path fileLevelPath = Preconditions
              .checkNotNull(timestampPath.getParent(), "file level path should not be null");
          try {
            if (Files.exists(fileLevelPath)) {
              //clear stale timestamp folders
              FileUtils.cleanDirectory(fileLevelPath.toFile());
            } else {
              Files.createDirectories(fileLevelPath);
            }
          } catch (IOException e) {
            LOG.debug("{} has been deleted or created, likely due to a benign race",
                fileLevelPath);
          }
          //TODO(beinan): risk of race condition, the pageInfo with an older fileInfo might win.
          // But we always have a most up-to-date fileInfo in the memory,
          // we can still guarantee the freshness of the pages returned by the LocalCacheManager
          createFileMetadata(pageInfo);
          Files.createDirectories(timestampPath);
        }
        Files.createFile(pagePath);
      }
      // extra try to ensure output stream is closed
      try (FileOutputStream fos = new FileOutputStream(pagePath.toFile(), false)) {
        fos.write(page);
      }
    } catch (Exception e) {
      Files.deleteIfExists(pagePath);
      if (e.getMessage().contains(ERROR_NO_SPACE_LEFT)) {
        throw new ResourceExhaustedException(
            String.format("%s is full, configured with %d bytes", getRoot(pageId), mCapacity), e);
      }
      throw new IOException("Failed to write file " + timestampPath + " for page " + pageId, e);
    }
  }

  private void createFileMetadata(PageInfo pageInfo) throws IOException {
    Path metaDataPath = getFileMetaDataPath(pageInfo.getPageId());
    Builder fileMetaBuilder = ProtoFileMeta.newBuilder();
    fileMetaBuilder.setLastModificationTime(pageInfo.getFileInfo().getLastModificationTimeMs());
    fileMetaBuilder.setScope(pageInfo.getFileInfo().getScope().getScopeId());
    try (FileOutputStream fos = new FileOutputStream(metaDataPath.toFile(), false)) {
      fileMetaBuilder.build().writeTo(fos);
    }
  }

  @Override
  public int get(PageInfo pageInfo, int pageOffset, int bytesToRead,
      byte[] buffer, int bufferOffset)
      throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    Preconditions.checkArgument(buffer.length >= bufferOffset,
        "page offset %s should be " + "less or equal than buffer length %s", bufferOffset,
        buffer.length);
    Path p = getPageFilePath(pageInfo);
    if (!Files.exists(p)) {
      throw new PageNotFoundException(p.toString());
    }
    long pageLength = p.toFile().length();
    Preconditions.checkArgument(pageOffset <= pageLength, "page offset %s exceeded page size %s",
        pageOffset, pageLength);
    try (RandomAccessFile localFile = new RandomAccessFile(p.toString(), "r")) {
      int bytesSkipped = localFile.skipBytes(pageOffset);
      if (pageOffset != bytesSkipped) {
        throw new IOException(
            String.format("Failed to read page %s (%s) from offset %s: %s bytes skipped",
                pageInfo.getPageId(), p,
                pageOffset, bytesSkipped));
      }
      int bytesRead = 0;
      int bytesLeft = (int) Math.min(pageLength - pageOffset, buffer.length - bufferOffset);
      bytesLeft = Math.min(bytesLeft, bytesToRead);
      while (bytesLeft >= 0) {
        int bytes = localFile.read(buffer, bufferOffset + bytesRead, bytesLeft);
        if (bytes <= 0) {
          break;
        }
        bytesRead += bytes;
        bytesLeft -= bytes;
      }
      return bytesRead;
    }
  }

  @Override
  public void delete(PageInfo pageInfo)
      throws IOException, PageNotFoundException {
    Path p = getPageFilePath(pageInfo);
    if (!Files.exists(p)) {
      throw new PageNotFoundException(p.toString());
    }
    Files.delete(p);
    // Cleaning up parent directory may lead to a race condition if one thread is removing a page as
    // well as its parent dir corresponding to the fileId, while another thread is adding
    // a different page from the same file in the same directory.
    // Note that, because (1) the chance of this type of racing is really low and
    // (2) even a race happens, the only penalty is an extra cache put failure;
    // whereas without the cleanup, there can be an unbounded amount of empty directories
    // uncleaned which takes an unbounded amount of space possibly.
    // We have seen the overhead goes up to a few hundred GBs due to inode storage overhead
    // TODO(binfan): remove the coupled fileId/pagIdex encoding with storage path, so the total
    // number of directories can be bounded.
    Path parent =
        Preconditions.checkNotNull(p.getParent(), "parent of cache file should not be null");
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
      if (!stream.iterator().hasNext()) {
        Files.delete(parent);
      }
    }
  }

  /**
   * @param pageInfo page info
   * @return the local file system path to store this page
   */
  @VisibleForTesting
  public Path getPageFilePath(PageInfo pageInfo) {
    PageId pageId = pageInfo.getPageId();
    // TODO(feng): encode fileId with URLEncoder to escape invalid characters for file name
    return Paths.get(getRoot(pageId).toString(), Long.toString(mPageSize),
        getFileBucket(pageId.getFileId()),
        pageId.getFileId(), Long.toString(pageInfo.getFileInfo().getLastModificationTimeMs()),
        Long.toString(pageId.getPageIndex()));
  }

  /**
   * @param pageId page Id
   * @return the local file system path to store the metadata of the file of this page
   */
  @VisibleForTesting
  public Path getFileMetaDataPath(PageId pageId) {
    // TODO(feng): encode fileId with URLEncoder to escape invalid characters for file name
    return Paths.get(getRoot(pageId).toString(), Long.toString(mPageSize),
        getFileBucket(pageId.getFileId()),
        pageId.getFileId(), FILE_METADATA_FILENAME);
  }

  private Path getRoot(PageId pageId) {
    // TODO(maobaolong): Refactor to support choose volume policy
    int index = pageId.hashCode() % mRoots.size();
    index = index < 0 ? index + mRoots.size() : index;
    return mRoots.get(index);
  }

  private String getFileBucket(String fileId) {
    return Integer.toString(Math.floorMod(fileId.hashCode(), mFileBuckets));
  }

  /**
   * @param path path of a file
   * @return the corresponding page id, or null if the file name does not match the pattern
   */
  @Nullable
  private PageId getPageId(Path path) {
    Matcher matcher = mPagePattern.matcher(path.toString());
    if (!matcher.matches()) {
      return null;
    }
    try {
      String fileBucket = Preconditions.checkNotNull(matcher.group(2));
      String fileId = Preconditions.checkNotNull(matcher.group(3));
      if (!fileBucket.equals(getFileBucket(fileId))) {
        return null;
      }
      String fileName = Preconditions.checkNotNull(matcher.group(5));
      long pageIndex = Long.parseLong(fileName);
      return new PageId(fileId, pageIndex);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * @param path path of a file
   * @return the corresponding page info for the file otherwise null
   */
  @Nullable
  private PageInfo getPageInfo(Path path, FileInfo fileInfo) {
    PageId pageId = getPageId(path);
    long pageSize;
    if (pageId == null) {
      LOG.error("Unrecognized page file" + path);
      return null;
    }
    try {
      pageSize = Files.size(path);
    } catch (IOException e) {
      LOG.error("Failed to get file size for " + path, e);
      return null;
    }
    return new PageInfo(pageId, pageSize, fileInfo);
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public Stream<PageInfo> getPages() throws IOException {
    Stream<Path> stream = Stream.empty();
    for (Path root : mRoots) {
      stream = Stream.concat(stream, Files.walk(root));
    }
    return stream.filter(this::isFileLevelDir)
        .flatMap(pathToFileDir -> {
          try {
            FileInfo fileInfo = loadFileInfo(pathToFileDir);
            // remove all the stale sub-folder
            // the timestamp of which is not equal to the last modified time in fileInfo
            Files.list(pathToFileDir)
                .filter(path -> Files.isDirectory(path) && !String.valueOf(path.getFileName())
                    .equals(String.valueOf(fileInfo.getLastModificationTimeMs())))
                .forEach(path -> {
                  try {
                    FileUtils.cleanDirectory(path.toFile());
                  } catch (IOException e) {
                    LOG.error("Failed to remove the stale folder " + path, e);
                  }
                });
            return Files
                //only the pages stored in the sub-folder of lastModificationTimeMs would be visited
                .list(pathToFileDir.resolve(Long.toString(fileInfo.getLastModificationTimeMs())))
                .filter(Files::isRegularFile)
                .map(pathToPage -> getPageInfo(pathToPage, fileInfo));
          } catch (IOException e) {
            LOG.error("Failed to go through the pages under " + pathToFileDir, e);
            return Stream
                .empty(); //empty stream would be used which won't break the parent streaming.
          }
        });
  }

  private FileInfo loadFileInfo(Path path) throws IOException {
    File metaDataFile = new File(path.toFile(), FILE_METADATA_FILENAME);
    try (FileInputStream is = new FileInputStream(metaDataFile)) {
      ProtoFileMeta fileMetaData = ProtoFileMeta.parseFrom(is);
      return new FileInfo(
          fileMetaData.hasScope() ? CacheScope.create(fileMetaData.getScope()) : CacheScope.GLOBAL,
          fileMetaData.getLastModificationTime()
      );
    }
  }

  private boolean isFileLevelDir(Path path) {
    Matcher matcher = mFilePattern.matcher(path.toString());
    return matcher.matches();
  }

  @Override
  public long getCacheSize() {
    return mCapacity;
  }
}
