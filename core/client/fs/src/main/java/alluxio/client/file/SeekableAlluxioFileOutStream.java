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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Seekable;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.util.io.FileUtils;
import alluxio.wire.OperationId;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Supports seek method on top of FileOutStream. This output stream will skip writing data to
 * Alluxio but directly creating or writing to an file in UFS.
 * Note that, to make this experimental feature work, one must ensure
 * 1. the write type must be THROUGH.
 * 2. the UFS address must be like a local address (e.g., an NAS mount point)
 */
@NotThreadSafe
public class SeekableAlluxioFileOutStream extends FileOutStream implements Seekable {
  private static final Logger LOG = LoggerFactory.getLogger(SeekableAlluxioFileOutStream.class);
  private final String mUfsPath;
  private final AlluxioURI mAlluxioPath;
  private final FileSystemContext mContext;
  private final FileSystem mFileSystem;
  private final RandomAccessFile mLocalFile;
  // state of this output stream
  public long mPos;
  private boolean mCanceled;
  private boolean mClosed;
  private final Mode mMode;

  /**
   * Mode when open this file.
   */
  public enum Mode {
    OPEN, CREATE
  }

  /**
   * Creates a new file output stream with seek.
   *
   * @param path the file path
   * @param options the client options
   * @param context the file system context
   * @return a new file output stream with seek
   */
  public static SeekableAlluxioFileOutStream create(AlluxioURI path,
      OutStreamOptions options, FileSystemContext context) throws IOException {
    String ufsPath = options.getUfsPath();
    if (FileUtils.exists(ufsPath)) {
      throw new FileAlreadyExistsException(String.format("File %s already exists", ufsPath));
    }
    Path parent = Paths.get(ufsPath).getParent();
    Files.createDirectories(parent);
    RandomAccessFile localFile =  new RandomAccessFile(ufsPath, "rw");
    return new SeekableAlluxioFileOutStream(Mode.CREATE, path, ufsPath, context, null, localFile);
  }

  /**
   * Creates a file output stream with seek. This file must be existing.
   *
   * @param path the file path
   * @param status file status
   * @param fs the file system
   * @return a new file output stream with seek
   */
  public static SeekableAlluxioFileOutStream open(AlluxioURI path,
      URIStatus status, FileSystem fs) throws IOException {
    String ufsPath = status.getUfsPath();
    if (!FileUtils.exists(ufsPath)) {
      throw new IOException(String.format("Can not find file %s", ufsPath));
    }
    RandomAccessFile localFile =  new RandomAccessFile(ufsPath, "rw");
    return new SeekableAlluxioFileOutStream(Mode.OPEN, path, ufsPath, null, fs, localFile);
  }

  private SeekableAlluxioFileOutStream(Mode mode, AlluxioURI path, String ufsPath,
      FileSystemContext context, FileSystem fs, RandomAccessFile localFile) {
    mMode = mode;
    mAlluxioPath = path;
    mContext = context;
    mFileSystem = fs;
    mUfsPath = ufsPath;
    mLocalFile =  localFile;
    mPos = 0;
    mCanceled = false;
    mClosed = false;
    mBytesWritten = 0;
  }

  @Override
  public void write(int b) throws IOException {
    mLocalFile.write(b);
    mPos++;
    // bytesWritten is perhaps a confusing name in this case,
    // as we are using it to indicate file len effectively.
    mBytesWritten = Math.max(mPos, mBytesWritten);
  }

  @Override
  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    mLocalFile.write(b, off, len);
    mPos += len;
    // bytesWritten is perhaps a confusing name in this case,
    // as we are using it to indicate file len effectively.
    mBytesWritten = Math.max(mPos, mBytesWritten);
  }

  @Override
  public void seek(long pos) throws IOException {
    mLocalFile.seek(pos);
    mPos = pos;
    mBytesWritten = Math.max(mPos, mBytesWritten);
  }

  public void flush() throws IOException {
    // flush is noop for RandomAccessFile
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mLocalFile.close();
    if (mMode == Mode.CREATE) {
      CompleteFilePOptions.Builder optionsBuilder = CompleteFilePOptions.newBuilder()
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
              .setOperationId(new OperationId(UUID.randomUUID()).toFsProto()))
          .setUfsLength(mBytesWritten);
      try (CloseableResource<FileSystemMasterClient> masterClient = mContext
          .acquireMasterClientResource()) {
        masterClient.get().completeFile(mAlluxioPath, optionsBuilder.build());
      } finally {
        mClosed = true;
      }
    } else {
      ListStatusPOptions options = ListStatusPOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                .setSyncIntervalMs(0).build())
            .build();
      try {
        mFileSystem.loadMetadata(mAlluxioPath, options);
      } catch (Exception e) {
        throw new IOException("Failed to update " + mAlluxioPath, e);
      } finally {
        mClosed = true;
      }
    }
  }

  /**
   * @param length length in bytes of the out stream
   */
  public void setLength(long length) throws IOException {
    seek(length);
    mLocalFile.setLength(length);
  }

  @ThreadSafe
  private static final class Metrics {
    // Note that only counter can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
    private static final Counter BYTES_WRITTEN_UFS =
        MetricsSystem.counter(MetricKey.CLIENT_BYTES_WRITTEN_UFS.getName());

    private Metrics() {} // prevent instantiation
  }
}
