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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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

  /**
   * Creates a new instance of {@link LocalPageStore}.
   *
   * @param options options for the local page store
   */
  public LocalPageStore(LocalPageStoreOptions options) {
    mRoot = options.getRootDir();
    mPageSize = options.getPageSize();
    mCapacity = (long) (options.getCacheSize() / (1 + options.getOverheadRatio()));
    mFileBuckets = options.getFileBuckets();
  }

  @Override
  public void put(PageId pageId,
      byte[] page,
      boolean isTemporary) throws ResourceExhaustedException, IOException {
    Path p = getFilePath(pageId, isTemporary);
    try {
      if (!Files.exists(p)) {
        Path parent =
            Preconditions.checkNotNull(p.getParent(), "parent of cache file should not be null");
        Files.createDirectories(parent);
        Files.createFile(p);
      }
      // extra try to ensure output stream is closed
      try (FileOutputStream fos = new FileOutputStream(p.toFile(), false)) {
        fos.write(page);
      }
    } catch (Exception e) {
      Files.deleteIfExists(p);
      if (e.getMessage().contains(ERROR_NO_SPACE_LEFT)) {
        throw new ResourceExhaustedException(
            String.format("%s is full, configured with %d bytes", mRoot, mCapacity), e);
      }
      throw new IOException("Failed to write file " + p + " for page " + pageId);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int bufferOffset,
      boolean isTemporary) throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    Preconditions.checkArgument(buffer.length >= bufferOffset,
        "page offset %s should be " + "less or equal than buffer length %s", bufferOffset,
        buffer.length);
    Path p = getFilePath(pageId, isTemporary);
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
            String.format("Failed to read page %s (%s) from offset %s: %s bytes skipped", pageId, p,
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
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    Path p = getFilePath(pageId, false);
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

  @Override
  public void commit(String fileId) throws IOException {
    Path dstBucketDir = Paths.get(mRoot.toString(), Long.toString(mPageSize),
        getFileBucket(mFileBuckets, fileId));
    if (!Files.exists(dstBucketDir)) {
      Files.createDirectories(dstBucketDir);
    }
    Files.move(
        Paths.get(mRoot.toString(), Long.toString(mPageSize), TEMP_DIR, fileId),
        Paths.get(mRoot.toString(), Long.toString(mPageSize),
            getFileBucket(mFileBuckets, fileId), fileId), StandardCopyOption.ATOMIC_MOVE);
  }

  /**
   * @param pageId page Id
   * @param isTemporary
   * @return the local file system path to store this page
   */
  @VisibleForTesting
  public Path getFilePath(PageId pageId, boolean isTemporary) {
    String bucketName = isTemporary ? TEMP_DIR : getFileBucket(mFileBuckets, pageId.getFileId());
    // TODO(feng): encode fileId with URLEncoder to escape invalid characters for file name
    return Paths.get(mRoot.toString(), Long.toString(mPageSize),
        bucketName, pageId.getFileId(),
        Long.toString(pageId.getPageIndex()));
  }

  @Override
  public void close() {
    // no-op
  }
}
