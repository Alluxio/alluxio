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

package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.concurrent.LockMode;
import alluxio.exception.AlluxioException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.lock.FuseReadWriteLockManager;
import alluxio.resource.CloseableResource;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

/**
 * FUSE file stream for supporting random access.
 */
public class RandomAccessFuseFileStream implements FuseFileStream {

  private static final Logger LOG = LoggerFactory.getLogger(RandomAccessFuseFileStream.class);

  private static final int COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT = Constants.MB;

  private final FileSystem mFileSystem;
  private final AlluxioURI mURI;
  private File mTmpFile;
  private RandomAccessFile mRandomAccessFile;

  private volatile boolean mHasInitializedTmpFile;

  private final AuthPolicy mAuthPolicy;

  private final CloseableResource<Lock> mLockResource;

  private volatile boolean mClosed = false;

  public static RandomAccessFuseFileStream create(FileSystem fileSystem, AuthPolicy authPolicy,
      FuseReadWriteLockManager lockManager, AlluxioURI uri, int flags, long mode) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(authPolicy);
    Preconditions.checkNotNull(lockManager);
    Preconditions.checkNotNull(uri);
    // Make sure file is not being read/written by current FUSE
    CloseableResource<Lock> lockResource = lockManager.tryLock(uri.toString(), LockMode.WRITE);

    try {
      // Make sure file is not being written by other clients outside current FUSE
      Optional<URIStatus> status = AlluxioFuseUtils.getPathStatus(fileSystem, uri);
      if (status.isPresent() && !status.get().isCompleted()) {
        status = AlluxioFuseUtils.waitForFileCompleted(fileSystem, uri);
        if (!status.isPresent()) {
          throw new UnimplementedRuntimeException(String.format(
              "Failed to create fuse file out stream for %s: cannot concurrently write same file",
              uri));
        }
      }
      if (mode == AlluxioFuseUtils.MODE_NOT_SET_VALUE && status.isPresent()) {
        mode = status.get().getMode();
      }
      long fileLen = status.map(URIStatus::getLength).orElse(0L);
      if (status.isPresent()) {
        if (AlluxioFuseOpenUtils.containsTruncate(flags) || fileLen == 0) {
          // support OPEN(O_WRONLY | O_RDONLY) existing file + O_TRUNC to write
          // support create empty file then open for write/read_write workload
          RandomAccessFuseFileStream randomAccessFuseFileStream =
              new RandomAccessFuseFileStream(fileSystem, authPolicy, uri, lockResource);
          randomAccessFuseFileStream.truncate(0);
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
                + "Alluxio deleted the old file and created a new file for writing", uri, flags));
          }
          return randomAccessFuseFileStream;
        } else {
          // Support open(O_WRONLY | O_RDWR flag) - truncate(0) - write() workflow
          return new RandomAccessFuseFileStream(fileSystem, authPolicy, uri, lockResource);
        }
      } else {
        throw new UnimplementedRuntimeException(String.format("RandomAccessFuseFileStream can only"
            + " used on existing file"));
      }
    } catch (Throwable t) {
      lockResource.close();
      throw t;
    }
  }

  /**
   * FUSE file stream for supporting random access.
   *
   * @param fileSystem
   * @param uri
   * @throws IOException
   * @throws AlluxioException
   */
  public RandomAccessFuseFileStream(FileSystem fileSystem, AuthPolicy authPolicy, AlluxioURI uri,
                                    CloseableResource<Lock> lockResource) {
    mFileSystem = fileSystem;
    mAuthPolicy = authPolicy;
    mURI = uri;
    mLockResource = lockResource;
  }

  private void initTmpFile() {
    try {
      mTmpFile = File.createTempFile("alluxio-fuse-random-access", null);
      mRandomAccessFile = new RandomAccessFile(mTmpFile, "rw");
    } catch (IOException e) {
      LOG.error("Failed to create temporary file for {}. ", mURI);
    }

    try (FileOutputStream out = new FileOutputStream(mTmpFile);
         FileInStream is = mFileSystem.openFile(mURI)) {
      byte[] buf = new byte[COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT];
      int t = is.read(buf);
      while (t != -1) {
        out.write(buf, 0, t);
        t = is.read(buf);
      }
    } catch (IOException | AlluxioException e) {
      LOG.error("Failed to copy {} to local temporary file {}. ", mURI, mTmpFile.getPath(), e);
    }
    // mark as initialized
    mHasInitializedTmpFile = true;
  }

  private void initTmpFileIfNotInitialized() {
    if (!mHasInitializedTmpFile) {
      synchronized (this) {
        if (!mHasInitializedTmpFile) {
          initTmpFile();
        }
      }
    }
  }

  @Override
  public int read(ByteBuffer buf, long size, long offset) {
    initTmpFileIfNotInitialized();

    try {
      mRandomAccessFile.seek(offset);
      return mRandomAccessFile.read(buf.array(), 0, (int) size);
    } catch (IOException e) {
      LOG.error("Failed to read bytes from the temporary file {}. ", mTmpFile.getPath(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(ByteBuffer buf, long size, long offset) {
    initTmpFileIfNotInitialized();

    try {
      int sz = (int) size;
      final byte[] dest = new byte[sz];
      buf.get(dest, 0, sz);
      mRandomAccessFile.seek(offset);
      mRandomAccessFile.write(dest, 0, (int) size);
    } catch (Exception e) {
      LOG.error("Failed to write bytes to the temporary file {}. ", mTmpFile.getPath(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public FileStatus getFileStatus() {
    initTmpFileIfNotInitialized();

    try {
      return new FileStatus(mRandomAccessFile.length());
    } catch (Exception e) {
      LOG.error("Failed to get the length of the temporary file {}. " + mTmpFile.getPath(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() {
    initTmpFileIfNotInitialized();

    // TODO(JiamingMai): rename it to a tmp file before deleting the file
    try {
      mFileSystem.delete(mURI);
    } catch (IOException | AlluxioException e) {
      LOG.error("Failed to delete file {} ", mURI);
    }

    try (FileOutStream fos = mFileSystem.createFile(mURI)) {
      // record the position for setting it back later
      long pos = mRandomAccessFile.getFilePointer();
      mRandomAccessFile.seek(0);
      FileChannel channel = mRandomAccessFile.getChannel();
      ByteBuffer buf = ByteBuffer.allocate(COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT);
      while (channel.read(buf) != -1) {
        buf.flip();
        fos.write(buf.array(), 0, buf.limit());
      }
      fos.close();
      // set the position back after copying
      mRandomAccessFile.seek(pos);
    } catch (IOException | AlluxioException e) {
      LOG.error("Encountered exception when flushing the stream. ", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void truncate(long size) {
    initTmpFileIfNotInitialized();

    try {
      mRandomAccessFile.setLength(size);
    } catch (Exception e) {
      LOG.error("Failed to set length of mRandomAccessFile to {}. The temporary file path is {}. ",
          size, mTmpFile.getPath(), e);
      throw new RuntimeException();
    }
  }

  @Override
  public synchronized void close() {
    if (mClosed) {
      return;
    }
    mClosed = true;
    try {
      copyFromTmpFile();
    } finally {
      mLockResource.close();
    }
  }

  private void copyFromTmpFile() {
    if (mRandomAccessFile != null) {
      // TODO(JiamingMai): rename it to a tmp file before deleting the file
      try {
        mFileSystem.delete(mURI);
      } catch (IOException | AlluxioException e) {
        LOG.error("Failed to delete file {} ", mURI);
      }
      // write the contents of the temporary file back to the file
      try (FileOutStream fos = mFileSystem.createFile(mURI)) {
        mRandomAccessFile.seek(0);
        FileChannel channel = mRandomAccessFile.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT);
        while (channel.read(buf) != -1) {
          buf.flip();
          fos.write(buf.array(), 0, buf.limit());
        }
        mRandomAccessFile.close();
      } catch (IOException | AlluxioException e) {
        LOG.error("Encountered exception when closing the stream:\n {} ", e.getMessage(), e);
        throw new RuntimeException(e);
      }
    }
    if (mTmpFile != null) {
      mTmpFile.delete();
    }
  }
}