package alluxio.master.transport;

import java.util.concurrent.CompletableFuture;

public class GrpcMessagingThreadContext {

  default CompletableFuture<Void> execute(Runnable callback) {
    CompletableFuture<Void> future = new CompletableFuture();
    this.executor().execute(() -> {
      try {
        callback.run();
        future.complete((Object)null);
      } catch (Throwable var3) {
        future.completeExceptionally(var3);
      }

    });
    return future;
  }
}
