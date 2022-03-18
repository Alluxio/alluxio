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
import alluxio.network.protocol.databuffer.DataBuffer;

import com.codahale.metrics.Counter;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A wrapper class on {@link BlockWriter} with timeout.
 * Note that, this reader will not queue any request.
 */
public class TimeBoundBlockWriter extends BlockWriter {
  private final BlockWriter mBlockWriter;
  private final long mTimeoutMs;
  private final TimeLimiter mTimeLimter;
  private final ExecutorService mExecutorService;

  /**
   * Creates new time bounded block writer.
   *
   * @param blockWriter the block writer
   * @param timeoutMs timeout in milliseconds
   * @param maxTimeoutThreads maximum threads to execute read operations and timeout
   */
  public TimeBoundBlockWriter(BlockWriter blockWriter, long timeoutMs, int maxTimeoutThreads)
      throws IOException {
    mBlockWriter = blockWriter;
    mTimeoutMs = timeoutMs;
    mExecutorService = new ThreadPoolExecutor(Math.min(4, maxTimeoutThreads),
        maxTimeoutThreads, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    mTimeLimter = SimpleTimeLimiter.create(mExecutorService);
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    return callWithTimeout(() -> mBlockWriter.append(inputBuf));
  }

  @Override
  public long append(ByteBuf buf) throws IOException {
    return callWithTimeout(() -> mBlockWriter.append(buf));
  }

  @Override
  public long append(DataBuffer buffer) throws IOException {
    return callWithTimeout(() -> mBlockWriter.append(buffer));
  }

  @Override
  public long getPosition() {
    return mBlockWriter.getPosition();
  }

  @Override
  public WritableByteChannel getChannel() {
    return mBlockWriter.getChannel();
  }

  @Override
  public void close() throws IOException {
    mExecutorService.shutdown();
    mBlockWriter.close();
  }

  private <T> T callWithTimeout(Callable<T> callable) throws IOException {
    try {
      return mTimeLimter.callWithTimeout(callable, mTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted while writing block with timeout.", e);
    } catch (TimeoutException e) {
      Metrics.WRITE_TIMEOUT.inc();
      throw new IOException(e);
    } catch (RejectedExecutionException e) {
      Metrics.WRITE_THREAD_REJECTED.inc();
      throw new IOException(e);
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, IOException.class);
      throw new IOException(t);
    }
  }

  private static final class Metrics {
    private static final Counter WRITE_TIMEOUT =
        MetricsSystem.counter(MetricKey.WORKER_CACHE_WRITE_TIMEOUT.getName());
    private static final Counter WRITE_THREAD_REJECTED =
        MetricsSystem.counter(MetricKey.WORKER_CACHE_WRITE_THREADS_REJECTED.getName());
  }
}
