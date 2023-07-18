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
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.file.dora.DoraCacheClient;
import alluxio.client.file.dora.netty.NettyDataWriter;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a streaming API to write a file. This class wraps the BlockOutStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can write to
 * Alluxio space in the local machine or remote machines. If the {@link UnderStorageType} is
 * {@link UnderStorageType#SYNC_PERSIST}, another stream will write the data to the under storage
 * system.
 */
@NotThreadSafe
public class DoraFileOutStream extends FileOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(DoraFileOutStream.class);

  /**
   * Used to manage closeable resources.
   */
  private final Closer mCloser;
  private final AlluxioStorageType mAlluxioStorageType;
  private final UnderStorageType mUnderStorageType;
  private final FileSystemContext mContext;
  private final NettyDataWriter mNettyDataWriter;

  /**
   * Stream to the file in the under storage, null if not writing to the under storage.
   */
  private final FileOutStream mUnderStorageOutputStream;

  private final OutStreamOptions mOptions;

  private boolean mCanceled;
  private boolean mClosed;

  protected final AlluxioURI mUri;

  private final DoraCacheClient mDoraClient;

  private final String mUuid;

  private final boolean mClientWriteToUFSEnabled;

  /**
   * Creates a new file output stream.
   *
   * @param doraClient   the dora client for requesting dora worker
   * @param dataWriter   the netty data writer which is used for transferring data with netty
   * @param path         the file path
   * @param options      the client options
   * @param context      the file system context
   * @param ufsOutStream the UfsOutStream for writing data to UFS
   * @param uuid         the UUID of a certain OutStream
   */
  public DoraFileOutStream(DoraCacheClient doraClient, NettyDataWriter dataWriter, AlluxioURI path,
                           OutStreamOptions options, FileSystemContext context,
                           @Nullable FileOutStream ufsOutStream, String uuid)
      throws IOException {
    mDoraClient = doraClient;
    mNettyDataWriter = dataWriter;
    mCloser = Closer.create();
    mUuid = uuid;
    // Acquire a resource to block FileSystemContext reinitialization, this needs to be done before
    // using mContext.
    // The resource will be released in close().
    mContext = context;
    mCloser.register(mContext.blockReinit());
    mClientWriteToUFSEnabled = context.getClusterConf()
        .getBoolean(PropertyKey.CLIENT_WRITE_TO_UFS_ENABLED);

    try {
      mUri = Preconditions.checkNotNull(path, "path");
      mAlluxioStorageType = options.getAlluxioStorageType();
      mUnderStorageType = options.getUnderStorageType();
      mOptions = options;
      mClosed = false;
      mCanceled = false;
      mBytesWritten = 0;

      if (mUnderStorageType.isSyncPersist()) {
        // Write is through to the under storage, create mUnderStorageOutputStream.
        mUnderStorageOutputStream = ufsOutStream;
      } else {
        mUnderStorageOutputStream = null;
      }
    } catch (Throwable t) {
      throw CommonUtils.closeAndRethrow(mCloser, t);
    }
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
    try (Timer.Context ctx = MetricsSystem
        .uniformTimer(MetricKey.CLOSE_ALLUXIO_OUTSTREAM_LATENCY.getName()).time()) {
      try {
        if (mAlluxioStorageType.isStore()) {
          if (mCanceled) {
            mNettyDataWriter.cancel();
          } else {
            mNettyDataWriter.flush();
          }
        }
      } catch (Exception e) {
        // Ignore.
      } finally {
        try {
          mNettyDataWriter.close();
        } catch (Exception ex) {
          // Ignore
        }
      }

      if (mUnderStorageType.isSyncPersist()) {
        try {
          if (mUnderStorageOutputStream != null) {
            if (mCanceled) {
              mUnderStorageOutputStream.cancel();
            } else {
              mUnderStorageOutputStream.flush();
            }
          }
        } catch (Exception e) {
          // Ignore;
        } finally {
          // Only close this output stream when write is enabled.
          // Otherwise this outputStream is used by client/ufs direct write.
          try {
            if (mUnderStorageOutputStream != null) {
              mUnderStorageOutputStream.close();
            }
          } catch (Exception e) {
            // Ignore;
          }
        }
      }

      CompleteFilePOptions options = CompleteFilePOptions.newBuilder()
          .setUfsLength(mNettyDataWriter.pos())
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().build())
          .setContentHash("HASH-256") // compute hash here
          .build();
      mClosed = true;
      mDoraClient.completeFile(mUri.toString(), options, mUuid);
    } catch (Exception e) {
      // Ignore.
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  @Override
  public void flush() throws IOException {
    mNettyDataWriter.flush();
    if (mUnderStorageType.isSyncPersist()) {
      if (mUnderStorageOutputStream != null) {
        mUnderStorageOutputStream.flush();
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    writeInternal(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    writeInternal(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    writeInternal(b, off, len);
  }

  private void writeInternal(int b) throws IOException {
    Integer intVal = b;
    byte[] bytes = new byte[] {intVal.byteValue()};
    writeInternal(bytes, 0, 1);
  }

  private void writeInternal(byte[] b, int off, int len) throws IOException {
    try {
      Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
      Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
          PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);

      if (mAlluxioStorageType.isStore() || !mClientWriteToUFSEnabled) {
        // If client is configured to write data to worker and ask worker to write to UFS,
        // client must send data over netty.
        mNettyDataWriter.writeChunk(b, off, len);

        Metrics.BYTES_WRITTEN_ALLUXIO.inc(len);
      }
      if (mUnderStorageType.isSyncPersist()) {
        if (mUnderStorageOutputStream != null) {
          mUnderStorageOutputStream.write(b, off, len);
          Metrics.BYTES_WRITTEN_UFS.inc(len);
        }
      }
      mBytesWritten += len;
    } catch (IOException e) {
      StringBuffer exceptionMsg = new StringBuffer();
      Throwable throwable = mNettyDataWriter.getPacketWriteException();
      if (throwable != null) {
        exceptionMsg.append(throwable.getMessage() + " ");
      }
      if (e.getMessage() != null) {
        exceptionMsg.append(e.getMessage());
      }
      throw new IOException(exceptionMsg.toString());
    }
  }

  /**
   * Class that contains metrics about FileOutStream.
   */
  @ThreadSafe
  private static final class Metrics {
    // Note that only counter can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
    private static final Counter BYTES_WRITTEN_ALLUXIO =
        MetricsSystem.counter(MetricKey.CLIENT_BYTES_WRITTEN_ALLUXIO.getName());
    private static final Counter BYTES_WRITTEN_UFS =
        MetricsSystem.counter(MetricKey.CLIENT_BYTES_WRITTEN_UFS.getName());

    private Metrics() {
    } // prevent instantiation
  }
}
