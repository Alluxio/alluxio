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

package alluxio.master.transport;

import com.google.common.base.Preconditions;
import io.atomix.catalyst.serializer.Serializer;
import org.apache.http.concurrent.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The context for Grpc messaging thread.
 */
public class GrpcMessagingContext {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingContext.class);
  private final ScheduledExecutorService mExecutor;
  private final Serializer mSerializer;
  private volatile boolean mBlocked;
  private final Executor mWrappedExecutor;

  /**
   * Constructs a new {@link GrpcMessagingContext}.
   *
   * @param nameFormat the name format
   * @param serializer the serializer
   */
  public GrpcMessagingContext(String nameFormat, Serializer serializer) {
    this(new GrpcMessagingThreadFactory(nameFormat), serializer);
  }

  /**
   * Constructs a new {@link GrpcMessagingContext}.
   *
   * @param factory factory to use when creating the new thread
   * @param serializer the serializer
   */
  public GrpcMessagingContext(GrpcMessagingThreadFactory factory, Serializer serializer) {
    this((ScheduledExecutorService) (new ScheduledThreadPoolExecutor(1, factory)), serializer);
  }

  /**
   * Constructs a new {@link GrpcMessagingContext}.
   *
   * @param executor the executor
   * @param serializer the serializer
   */
  public GrpcMessagingContext(ScheduledExecutorService executor, Serializer serializer) {
    this(getThread(executor), executor, serializer);
  }

  /**
   * Constructs a new {@link GrpcMessagingContext}.
   *
   * @param thread the thread
   * @param executor the executor
   * @param serializer the serializer
   */
  public GrpcMessagingContext(Thread thread,
      ScheduledExecutorService executor, Serializer serializer) {
    mWrappedExecutor = new Executor() {
      public void execute(Runnable command) {
        try {
          GrpcMessagingContext.this.mExecutor.execute(logFailure(command));
        } catch (RejectedExecutionException var3) {
          // Ignore the rejected exception
        }
      }
    };
    mExecutor = executor;
    mSerializer = serializer;
    Preconditions.checkState(thread instanceof GrpcMessagingThread,
        "not a Grpc messaging thread");
    ((GrpcMessagingThread) thread).setContext(this);
  }

  protected static GrpcMessagingThread getThread(ExecutorService executor) {
    AtomicReference thread = new AtomicReference();

    try {
      executor.submit(() -> {
        thread.set((GrpcMessagingThread) Thread.currentThread());
      }).get();
    } catch (ExecutionException | InterruptedException var3) {
      throw new IllegalStateException("failed to initialize thread state", var3);
    }
    return (GrpcMessagingThread) thread.get();
  }

  /**
   * @return the serializer of this context
   */
  public Serializer serializer() {
    return mSerializer;
  }

  /**
   * @return the executor of this context
   */
  public Executor executor() {
    return mWrappedExecutor;
  }

  /**
   * Submits a one-shot task that becomes enabled after the given delay.
   *
   * @param delay the time from now to delay execution
   * @param runnable the task to execute
   * @return task cancellable
   */
  public Cancellable schedule(Duration delay, Runnable runnable) {
    ScheduledFuture<?> future = mExecutor.schedule(logFailure(runnable),
        delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> {
      return future.cancel(false);
    };
  }

  /**
   * Submits a one-shot task that becomes enabled after the given delay.
   *
   * @param delay the time from now to delay execution
   * @param interval the interval between successive executions
   * @param runnable the task to execute
   * @return task cancellable
   */
  public Cancellable schedule(Duration delay, Duration interval, Runnable runnable) {
    ScheduledFuture<?> future = mExecutor.scheduleAtFixedRate(logFailure(runnable),
        delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> {
      return future.cancel(false);
    };
  }

  /**
   * Closes the context.
   */
  public void close() {
    mExecutor.shutdownNow();
  }

  /**
   * Executes the given callback.
   *
   * @param callback the callback to execute
   * @return completable future of the callback run
   */
  public CompletableFuture<Void> execute(Runnable callback) {
    CompletableFuture<Void> future = new CompletableFuture();
    executor().execute(() -> {
      try {
        callback.run();
        future.complete(null);
      } catch (Throwable var3) {
        future.completeExceptionally(var3);
      }
    });
    return future;
  }

  /**
   * Executes the given callback.
   *
   * @param callback the callback to execute
   * @return completable future of the callback run
   * @param <T> the supplier type
   */
  public <T> CompletableFuture<T> execute(Supplier<T> callback) {
    CompletableFuture<T> future = new CompletableFuture();
    executor().execute(() -> {
      try {
        future.complete(callback.get());
      } catch (Throwable var3) {
        future.completeExceptionally(var3);
      }
    });
    return future;
  }

  static GrpcMessagingContext currentContext() {
    Thread thread = Thread.currentThread();
    return thread instanceof GrpcMessagingThread
        ? ((GrpcMessagingThread) thread).getContext() : null;
  }

  static GrpcMessagingContext currentContextOrThrow() {
    GrpcMessagingContext context = currentContext();
    Preconditions.checkNotNull(context, "not on a Grpc messaging thread");
    return context;
  }

  Runnable logFailure(Runnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable var3) {
        if (!(var3 instanceof RejectedExecutionException)) {
          LOG.error("An uncaught exception occurred", var3);
        }

        throw var3;
      }
    };
  }
}
