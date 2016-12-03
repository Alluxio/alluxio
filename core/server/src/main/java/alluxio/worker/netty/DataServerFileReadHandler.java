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

package alluxio.worker.netty;

import alluxio.Constants;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBuffer;
import alluxio.worker.file.FileSystemWorker;

import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockReadRequest}s.
 */
@NotThreadSafe
final public class DataServerFileReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final FileSystemWorker mWorker;

  /**
   * The block read request internal representation.
   */
  private final class FileReadRequestInternal extends ReadRequestInternal {
    /** The UFS input stream. No need to close. */
    public InputStream mInputStream = null;

    /**
     * Creates an instance of {@link FileReadRequestInternal}.
     *
     * @param request the block read request
     * @throws Exception if it fails to create the object
     */
    public FileReadRequestInternal(RPCBlockReadRequest request) throws Exception {
      mInputStream = mWorker.getUfsInputStream(request.getBlockId(), request.getOffset());

      mStart = request.getOffset();
      mEnd = mStart + request.getLength();
    }

    @Override
    public void close() {}
  }

  /**
   * Creates an instance of {@link DataServerReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s.
   * @param worker the file system worker
   */
  public DataServerFileReadHandler(ExecutorService executorService, FileSystemWorker worker) {
    super(executorService);
    mWorker = worker;
  }

  @Override
  protected void initializeRequest(RPCBlockReadRequest request) throws Exception {
    mRequest = new FileReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(long offset, int len) throws IOException {
    byte[] data = new byte[len];
    InputStream in = ((FileReadRequestInternal) mRequest).mInputStream;

    int bytesRead = 0;
    if (in != null) { // if we have not reached the end of the file
      while (bytesRead < len) {
        int read = in.read(data, bytesRead, len - bytesRead);
        if (read == -1) {
          break;
        }
        bytesRead += read;
      }
    }
    return bytesRead != 0 ?
        new DataNettyBuffer(Unpooled.wrappedBuffer(data, 0, bytesRead), bytesRead) : null;
  }
}
