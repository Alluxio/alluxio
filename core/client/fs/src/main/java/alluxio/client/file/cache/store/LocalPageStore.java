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

import static alluxio.client.file.cache.store.PageStoreDir.getFileBucket;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.commons.io.FileUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link LocalPageStore} is an implementation of {@link PageStore} which
 * stores all pages in a directory somewhere on the local disk.
 */
@NotThreadSafe
public class LocalPageStore implements PageStore {
  private static final String ERROR_NO_SPACE_LEFT = "No space left on device";
  public static final String TEMP_DIR = "TEMP";
  private final Path mRoot;
  private final long mPageSize;
  private final long mCapacity;
  private final int mFileBuckets;
  private final LoadingCache<String, RandomAccessFile> mOpenedReadFile;

  /**
   * Creates a new instance of {@link LocalPageStore}.
   *
   * @param options options for the local page store
   */
  public LocalPageStore(PageStoreOptions options) {
    mRoot = options.getRootDir();
    mPageSize = options.getPageSize();
    mCapacity = (long) (options.getCacheSize() / (1 + options.getOverheadRatio()));
    mFileBuckets = options.getFileBuckets();
    mOpenedReadFile
        = CacheBuilder.newBuilder().weakValues()
        .removalListener(
            new RemovalListener<String, RandomAccessFile>(){
              @Override
              public void onRemoval(RemovalNotification<String, RandomAccessFile> notification) {
                try {
                  assert notification.getValue() != null;
                  notification.getValue().close();
                } catch (IOException e) {
                  LOG.error("Failed to close random access file of {} when removing the entry",
                      notification.getKey());
                  throw new RuntimeException(e);
                }
              }
            })
        .build(new CacheLoader<String, RandomAccessFile>() {
          @Override
          public RandomAccessFile load(String key) throws FileNotFoundException {
            return new RandomAccessFile(key, "r");
          }
        });
  }

  @Override
  public void put(PageId pageId,
      ByteBuffer page,
      boolean isTemporary) throws ResourceExhaustedException, IOException {
    Path pagePath = getPagePath(pageId, isTemporary);
    mOpenedReadFile.invalidate(pagePath.toString());
    try {
      if (!Files.exists(pagePath)) {
        Path parent = Preconditions.checkNotNull(pagePath.getParent(),
            "parent of cache file should not be null");
        Files.createDirectories(parent);
        Files.createFile(pagePath);
      }
      // extra try to ensure output stream is closed
      try (FileOutputStream fos = new FileOutputStream(pagePath.toFile(), false)) {
        fos.getChannel().write(page);
      }
    } catch (Exception e) {
      Files.deleteIfExists(pagePath);
      if (e.getMessage().contains(ERROR_NO_SPACE_LEFT)) {
        throw new ResourceExhaustedException(
            String.format("%s is full, configured with %d bytes", mRoot, mCapacity), e);
      }
      throw new IOException("Failed to write file " + pagePath + " for page " + pageId);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, PageReadTargetBuffer target,
      boolean isTemporary) throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    Path pagePath = getPagePath(pageId, isTemporary);
    String pagePathString = pagePath.toString();
    RandomAccessFile localFile;
    try {
      localFile = mOpenedReadFile.get(pagePathString);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof FileNotFoundException) {
        throw new PageNotFoundException(pagePath.toString(), e.getCause());
      }
      throw new IOException(e);
    }
    try {
      localFile.seek(pageOffset);
      int bytesRead = 0;
      int bytesLeft = Math.min((int) target.remaining(), bytesToRead);
      while (bytesLeft > 0) {
        int bytes = target.readFromFile(localFile, bytesLeft);
        if (bytes <= 0) {
          break;
        }
        bytesRead += bytes;
        bytesLeft -= bytes;
      }
      return bytesRead;
    } catch (Throwable t) {
      mOpenedReadFile.invalidate(pagePathString);
      throw t;
    }
  }

  @Override
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    Path pagePath = getPagePath(pageId, false);
    mOpenedReadFile.invalidate(pagePath.toString());
    if (!Files.exists(pagePath)) {
      throw new PageNotFoundException(pagePath.toString());
    }
    Files.delete(pagePath);
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
        Preconditions.checkNotNull(pagePath.getParent(), "parent of cache file should not be null");
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
      if (!stream.iterator().hasNext()) {
        Files.delete(parent);
      }
    } catch (NoSuchFileException e) {
      //Parent path is deleted by other thread in a benign race, ignore exception and continue
      if (LOG.isDebugEnabled()) {
        LOG.debug("Parent path is deleted by other thread, ignore and continue.", e);
      }
    }
  }

  @Override
  public void commit(String fileId, String newFileId) throws IOException {
    Path filePath = getFilePath(newFileId);
    Path bucketPath = Preconditions.checkNotNull(filePath.getParent(),
        "%s does not have a parent path", filePath);
    if (!Files.exists(bucketPath)) {
      Files.createDirectories(bucketPath);
    }
    Path tempPath = getTempFilePath(fileId);
    mOpenedReadFile.invalidate(tempPath.toString());
    mOpenedReadFile.invalidate(filePath.toString());
    Files.move(
        tempPath,
        filePath, StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public void abort(String fileId) throws IOException {
    Path tempPath = getTempFilePath(fileId);
    mOpenedReadFile.invalidate(tempPath.toString());
    FileUtils.deleteDirectory(tempPath.toFile());
  }

  private Path getTempFilePath(String fileId) {
    return Paths.get(mRoot.toString(), Long.toString(mPageSize), TEMP_DIR, fileId);
  }

  private Path getFilePath(String fileId) {
    return Paths.get(mRoot.toString(), Long.toString(mPageSize),
        getFileBucket(mFileBuckets, fileId), fileId);
  }

  /**
   * @param pageId page Id
   * @param isTemporary
   * @return the local file system path to store this page
   */
  @VisibleForTesting
  public Path getPagePath(PageId pageId, boolean isTemporary) {
    // TODO(feng): encode fileId with URLEncoder to escape invalid characters for file name
    Path filePath =
        isTemporary ? getTempFilePath(pageId.getFileId()) : getFilePath(pageId.getFileId());
    return filePath.resolve(Long.toString(pageId.getPageIndex()));
  }

  @Override
  public void close() {
    // no-op
  }
}
