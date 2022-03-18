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

package alluxio.worker.block.io;

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A wrapper class on {@link BlockReader} with timeout.
 * Note that, this reader will not queue any request.
 */
public class TimeBoundBlockReader extends BlockReader {
  private final BlockReader mBlockReader;
  private final long mTimeoutMs;
  private TimeLimiter mTimeLimter = null;
  private ExecutorService mExecutorService = null;

  /**
   * Creates new time bounded block reader.
   *
   * @param blockReader the block reader
   * @param timeoutMs timeout in milliseconds
   * @param maxTimeoutThreads maximum threads to execute operation and timeout
   */
  public TimeBoundBlockReader(BlockReader blockReader, long timeoutMs, int maxTimeoutThreads)
      throws IOException {
    mBlockReader = blockReader;
    mTimeoutMs = timeoutMs;
    mExecutorService = new ThreadPoolExecutor(Math.min(4, maxTimeoutThreads),
        maxTimeoutThreads, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    mTimeLimter = SimpleTimeLimiter.create(mExecutorService);
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    return callWithTimeout(() -> mBlockReader.read(offset, length));
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    return callWithTimeout(() -> mBlockReader.transferTo(buf));
  }

  @Override
  public void close() throws IOException {
    mExecutorService.shutdown();
    mBlockReader.close();
  }

  @Override
  public long getLength() {
    return mBlockReader.getLength();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return mBlockReader.getChannel();
  }

  @Override
  public boolean isClosed() {
    return mBlockReader.isClosed();
  }

  @Override
  public String getLocation() {
    return mBlockReader.getLocation();
  }

  private <T> T callWithTimeout(Callable<T> callable) throws IOException {
    try {
      return mTimeLimter.callWithTimeout(callable, mTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted while reading block with timeout.", e);
    } catch (TimeoutException e) {
      Metrics.READ_TIMEOUT.inc();
      throw new IOException(e);
    } catch (RejectedExecutionException e) {
      Metrics.READ_THREAD_REJECTED.inc();
      throw new IOException(e);
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, IOException.class);
      throw new IOException(t);
    }
  }

  private static final class Metrics {
    private static final Counter READ_TIMEOUT =
        MetricsSystem.counter(MetricKey.WORKER_CACHE_READ_TIMEOUT.getName());
    private static final Counter READ_THREAD_REJECTED =
        MetricsSystem.counter(MetricKey.WORKER_CACHE_READ_THREADS_REJECTED.getName());
  }
}
