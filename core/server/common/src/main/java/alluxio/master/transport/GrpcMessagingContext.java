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

import com.google.common.base.Preconditions;
import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcMessagingContext {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingContext.class);
  private final ScheduledExecutorService executor;
  private final Serializer serializer;
  private volatile boolean blocked;
  private final Executor wrappedExecutor;

  public GrpcMessagingContext(String nameFormat, Serializer serializer) {
    this(new GrpcMessagingThreadFactory(nameFormat), serializer);
  }

  public GrpcMessagingContext(GrpcMessagingThreadFactory factory, Serializer serializer) {
    this((ScheduledExecutorService)(new ScheduledThreadPoolExecutor(1, factory)), serializer);
  }

  public GrpcMessagingContext(ScheduledExecutorService executor, Serializer serializer) {
    this(getThread(executor), executor, serializer);
  }

  public GrpcMessagingContext(Thread thread, ScheduledExecutorService executor, Serializer serializer) {
    this.wrappedExecutor = new Executor() {
      public void execute(Runnable command) {
        try {
          GrpcMessagingContext.this.executor.execute(logFailure(command));
        } catch (RejectedExecutionException var3) {
        }

      }
    };
    this.executor = executor;
    this.serializer = serializer;
    Preconditions.checkState(thread instanceof GrpcMessagingThread, "not a Grpc messaging thread", new Object[0]);
    ((GrpcMessagingThread)thread).setContext(this);
  }

  protected static GrpcMessagingThread getThread(ExecutorService executor) {
    AtomicReference thread = new AtomicReference();

    try {
      executor.submit(() -> {
        thread.set((GrpcMessagingThread)Thread.currentThread());
      }).get();
    } catch (ExecutionException | InterruptedException var3) {
      throw new IllegalStateException("failed to initialize thread state", var3);
    }

    return (GrpcMessagingThread)thread.get();
  }

  public void block() {
    this.blocked = true;
  }

  public void unblock() {
    this.blocked = false;
  }

  public boolean isBlocked() {
    return this.blocked;
  }

  public Logger logger() {
    return LOG;
  }

  public Serializer serializer() {
    return this.serializer;
  }

  public Executor executor() {
    return this.wrappedExecutor;
  }

  public Scheduled schedule(Duration delay, Runnable runnable) {
    ScheduledFuture<?> future = this.executor.schedule(logFailure(runnable), delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> {
      future.cancel(false);
    };
  }

  public Scheduled schedule(Duration delay, Duration interval, Runnable runnable) {
    ScheduledFuture<?> future = this.executor.scheduleAtFixedRate(logFailure(runnable), delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> {
      future.cancel(false);
    };
  }

  public void close() {
    this.executor.shutdownNow();
  }

  public CompletableFuture<Void> execute(Runnable callback) {
    CompletableFuture<Void> future = new CompletableFuture();
    this.executor().execute(() -> {
      try {
        callback.run();
        future.complete(null);
      } catch (Throwable var3) {
        future.completeExceptionally(var3);
      }

    });
    return future;
  }

  public <T> CompletableFuture<T> execute(Supplier<T> callback) {
    CompletableFuture<T> future = new CompletableFuture();
    this.executor().execute(() -> {
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
    return thread instanceof GrpcMessagingThread ? ((GrpcMessagingThread)thread).getContext() : null;
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
