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
import alluxio.exception.PageCorruptedException;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.file.ReadTargetBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;

import java.io.File;
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
  public LocalPageStore(PageStoreOptions options) {
    mRoot = options.getRootDir();
    mPageSize = options.getPageSize();
    mCapacity = (long) (options.getCacheSize() / (1 + options.getOverheadRatio()));
    mFileBuckets = options.getFileBuckets();
  }

  @Override
  public void put(PageId pageId,
      ByteBuffer page,
      boolean isTemporary) throws ResourceExhaustedException, IOException {
    Path pagePath = getPagePath(pageId, isTemporary);
    try {
      LOG.debug("Put page: {}, page's position: {}, page's limit: {}, page's capacity: {}",
          pageId, page.position(), page.limit(), page.capacity());
      if (!Files.exists(pagePath)) {
        Path parent = Preconditions.checkNotNull(pagePath.getParent(),
            "parent of cache file should not be null");
        Files.createDirectories(parent);
      }
      // extra try to ensure output stream is closed
      try (FileOutputStream fos = new FileOutputStream(pagePath.toFile(), false)) {
        fos.getChannel().write(page);
      }
    } catch (Throwable t) {
      Files.deleteIfExists(pagePath);
      if (t.getMessage() != null && t.getMessage().contains(ERROR_NO_SPACE_LEFT)) {
        throw new ResourceExhaustedException(
            String.format("%s is full, configured with %d bytes", mRoot, mCapacity), t);
      }
      throw new IOException("Failed to write file " + pagePath + " for page " + pageId, t);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer target,
      boolean isTemporary) throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    Preconditions.checkArgument(bytesToRead >= 0, "bytes to read should be non-negative");
    if (target.remaining() == 0 || bytesToRead == 0) {
      return 0;
    }
    Path pagePath = getPagePath(pageId, isTemporary);
    try (RandomAccessFile localFile = new RandomAccessFile(pagePath.toString(), "r")) {
      long pageLength = localFile.length();
      if (pageOffset + bytesToRead > pageLength) {
        throw new PageCorruptedException(String.format(
            "The page %s (%s) probably has been corrupted, "
                + "page-offset %s, bytes to read %s, page file length %s",
            pageId, pagePath, pageOffset, bytesToRead, pageLength));
      }
      int bytesSkipped = localFile.skipBytes(pageOffset);
      if (pageOffset != bytesSkipped) {
        throw new IOException(
            String.format("Failed to read page %s (%s) from offset %s: %s bytes skipped",
                pageId, pagePath, pageOffset, bytesSkipped));
      }
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
      if (bytesRead == 0) {
        SAMPLING_LOG.warn("Read 0 bytes from page {}, the page is probably empty", pageId);
        // no bytes have been read at all, but the requested length > 0
        // this means the file is empty
        return -1;
      }
      return bytesRead;
    } catch (FileNotFoundException e) {
      throw new PageNotFoundException(pagePath.toString());
    }
  }

  /**
   *
   * @param pageId page identifier
   * @param isTemporary whether is to delete a temporary page or not
   * @throws IOException
   * @throws PageNotFoundException
   */
  public void delete(PageId pageId, boolean isTemporary) throws IOException, PageNotFoundException {
    Path pagePath = getPagePath(pageId, isTemporary);
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
    Files.move(
        getTempFilePath(fileId),
        filePath, StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public void abort(String fileId) throws IOException {
    FileUtils.deleteDirectory(getTempFilePath(fileId).toFile());
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
  public DataFileChannel getDataFileChannel(
      PageId pageId, int pageOffset, int bytesToRead, boolean isTemporary)
      throws PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0,
        "page offset should be non-negative");
    Preconditions.checkArgument(!isTemporary,
        "cannot acquire a data file channel to a temporary page");
    Path pagePath = getPagePath(pageId, isTemporary);
    File pageFile = pagePath.toFile();
    if (!pageFile.exists()) {
      throw new PageNotFoundException(pagePath.toString());
    }

    long fileLength = pageFile.length();
    if (fileLength == 0 && pageId.getPageIndex() > 0) {
      // pages other than the first page should always be non-empty
      // remove this malformed page
      SAMPLING_LOG.warn("Length of page {} is 0, removing this malformed page", pageId);
      try {
        Files.deleteIfExists(pagePath);
      } catch (IOException ignored) {
        // do nothing
      }
      throw new PageNotFoundException(pagePath.toString());
    }
    if (fileLength < pageOffset) {
      throw new IllegalArgumentException(
          String.format("offset %s exceeds length of page %s", pageOffset, fileLength));
    }
    if (pageOffset + bytesToRead > fileLength) {
      bytesToRead = (int) (fileLength - (long) pageOffset);
    }

    DataFileChannel dataFileChannel = new DataFileChannel(pageFile, pageOffset, bytesToRead);
    return dataFileChannel;
  }

  @Override
  public void close() {
    // no-op
  }
}
