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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.concurrent.LockMode;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.file.FileStatus;
import alluxio.fuse.file.FuseFileStream;
import alluxio.fuse.lock.FuseReadWriteLockManager;
import alluxio.resource.CloseableResource;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
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

public class RandomAccessFuseFileStream implements FuseFileStream {

  private static final Logger LOG = LoggerFactory.getLogger(RandomAccessFuseFileStream.class);

  private static final int BUFFER_SIZE = Configuration.getInt(
      PropertyKey.FUSE_RANDOM_ACCESS_FILE_STREAM_BUFFER_SIZE);

  private final int mFileSizeWarningThreshold = Constants.GB;

  private final FileSystem mFileSystem;
  private final AlluxioURI mURI;
  private File mTmpFile;
  private RandomAccessFile mRandomAccessFile;

  private volatile boolean mHasInitializedTmpFile;

  private final AuthPolicy mAuthPolicy;

  private final CloseableResource<Lock> mLockResource;

  private volatile boolean mClosed = false;
  private volatile FileStatus mFinalFileStatusAfterClose = null;

  /**
   * Create an instance of {@link RandomAccessFuseFileStream}.
   *
   * @param fileSystem  the file system on which the fuse file stream is based
   * @param authPolicy  the authority policy for the file system
   * @param lockManager the lock manager
   * @param uri         the alluxio uri
   * @param flags       the fuse create/open flags
   * @param mode        the filesystem mode, -1 if not set
   * @return an instance of {@link RandomAccessFuseFileStream}
   */
  public static RandomAccessFuseFileStream create(FileSystem fileSystem, AuthPolicy authPolicy,
                                                  FuseReadWriteLockManager lockManager,
                                                  AlluxioURI uri, int flags, long mode) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(authPolicy);
    Preconditions.checkNotNull(lockManager);
    Preconditions.checkNotNull(uri);
    // Make sure file is not being read/written by current FUSE
    CloseableResource<Lock> lockResource = lockManager.tryLock(uri.toString(), LockMode.READ);

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
      if (checkIfUfsFileTooLarge(mURI)) {
        LOG.warn("The file {} to be accessed is larger than {} bytes. Using "
                + " RandomAccessFuseFileStream may introduce significant overhead. ",
            mURI, mFileSizeWarningThreshold);
      }
      byte[] buf = new byte[BUFFER_SIZE];
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

  private boolean checkIfUfsFileTooLarge(AlluxioURI uri) throws IOException, AlluxioException {
    URIStatus uriStatus = mFileSystem.getStatus(uri);
    long fileSize = uriStatus.getLength();
    if (fileSize >= mFileSizeWarningThreshold) {
      return true;
    } else {
      return false;
    }
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
  public synchronized int read(ByteBuffer buf, long size, long offset) {
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
  public synchronized void write(ByteBuffer buf, long size, long offset) {
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
  public synchronized FileStatus getFileStatus() {
    if (mClosed) {
      return mFinalFileStatusAfterClose;
    }

    initTmpFileIfNotInitialized();

    try {
      return new FileStatus(mRandomAccessFile.length());
    } catch (Exception e) {
      LOG.error("Failed to get the length of the temporary file {}. " + mTmpFile.getPath(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void flush() {
    initTmpFileIfNotInitialized();
    copyFromTmpFile();
  }

  @Override
  public synchronized void truncate(long size) {
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
      mFinalFileStatusAfterClose = new FileStatus(mRandomAccessFile.length());
      mRandomAccessFile.close();
      if (mTmpFile != null) {
        mTmpFile.delete();
      }
    } catch (IOException e) {
      LOG.error("Encountered exception when closing the stream:\n {} ", e.getMessage(), e);
    } finally {
      mLockResource.close();
    }
  }

  @Override
  public boolean allowWrite() {
    return true;
  }

  private void copyFromTmpFile() {
    if (mRandomAccessFile != null) {
      // rename it to a tmp file before deleting the file
      String randomSuffix =
          String.format(".%s_copyToLocal_", RandomStringUtils.randomAlphanumeric(8));
      AlluxioURI backupUri = new AlluxioURI(mURI.getPath() + randomSuffix);
      try {
        mFileSystem.rename(mURI, backupUri);
      } catch (IOException | AlluxioException e) {
        LOG.error("Failed to rename file {} to {}", mURI, backupUri, e);
      }
      // write the contents of the temporary file back to the file
      try (FileOutStream fos = mFileSystem.createFile(mURI)) {
        long pos = mRandomAccessFile.getFilePointer();
        mRandomAccessFile.seek(0);
        FileChannel channel = mRandomAccessFile.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
        while (channel.read(buf) != -1) {
          buf.flip();
          fos.write(buf.array(), 0, buf.limit());
        }
        // set the position back after copying
        mRandomAccessFile.seek(pos);
        // delete the renamed file after writing the original file successfully
        if (mFileSystem.exists(backupUri)) {
          mFileSystem.delete(backupUri);
        }
      } catch (IOException | AlluxioException e) {
        LOG.error("Encountered exception when copying temporary file:\n {} ", e.getMessage(), e);
        try {
          if (mFileSystem.exists(backupUri)) {
            mFileSystem.rename(backupUri, mURI);
          }
        } catch (IOException | AlluxioException ex) {
          LOG.warn("Failed to rename file {} to {}.", backupUri, mURI, ex);
        }
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }
}
