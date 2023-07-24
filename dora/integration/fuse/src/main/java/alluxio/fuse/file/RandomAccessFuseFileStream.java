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
import alluxio.exception.AlluxioException;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.fuse.AlluxioFuseUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

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

  /**
   * FUSE file stream for supporting random access.
   *
   * @param fileSystem
   * @param uri
   * @throws IOException
   * @throws AlluxioException
   */
  public RandomAccessFuseFileStream(FileSystem fileSystem, AlluxioURI uri) {
    mFileSystem = fileSystem;
    mURI = uri;
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

    /*
    if (size == 0) {
      try {
        mRandomAccessFile.close();
      } catch (IOException e) {

      } finally {
        mTmpFile.delete();
      }

      AlluxioFuseUtils.deletePath(mFileSystem, mURI);

      try (FileOutStream fos = mFileSystem.createFile(mURI)) {
        mHasInitializedTmpFile = false;
      } catch (IOException | AlluxioException e) {
      }
      return;
    }

    try {
      if (size > mRandomAccessFile.length()) {
        throw new UnimplementedRuntimeException(
            String.format("Cannot truncate file %s from size %s to size %s", mURI, size,
                size));
      }
    } catch (IOException e) {
      LOG.error("Failed to get mRandomAccessFile.length(). The temporary file path is {}. ",
          mTmpFile, e);
    }
    */

    try {
      mRandomAccessFile.setLength(size);
    } catch (Exception e) {
      LOG.error("Failed to set length of mRandomAccessFile to {}. The temporary file path is {}. ",
          size, mTmpFile.getPath(), e);
      throw new RuntimeException();
    }
  }

  @Override
  public void close() {
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
