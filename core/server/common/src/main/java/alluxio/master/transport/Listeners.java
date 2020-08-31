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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

public class Listeners<T> implements Iterable<Listeners<T>.ListenerHolder> {
  private final List<Listeners<T>.ListenerHolder> listeners = new CopyOnWriteArrayList();

  public Listeners() {
  }

  public int size() {
    return listeners.size();
  }

  public ListenerHolder add(Consumer<T> listener) {
    Preconditions.checkNotNull(listener, "listener should not be null");
    Listeners<T>.ListenerHolder holder = new Listeners.ListenerHolder(listener, GrpcMessagingContext.currentContext());
    listeners.add(holder);
    return holder;
  }

  public CompletableFuture<Void> accept(T event) {
    List<CompletableFuture<Void>> futures = new ArrayList(this.listeners.size());
    Iterator var3 = listeners.iterator();

    while(var3.hasNext()) {
      Listeners<T>.ListenerHolder listener = (Listeners.ListenerHolder)var3.next();
      if (listener.context != null) {
        futures.add(listener.context.execute(() -> {
          listener.listener.accept(event);
        }));
      } else {
        listener.listener.accept(event);
      }
    }

    return CompletableFuture.allOf((CompletableFuture[])futures.toArray(new CompletableFuture[futures.size()]));
  }

  public Iterator<Listeners<T>.ListenerHolder> iterator() {
    return listeners.iterator();
  }

  public class ListenerHolder implements Consumer<T>, AutoCloseable {
    private final Consumer<T> listener;
    private final GrpcMessagingContext context;

    private ListenerHolder(Consumer<T> listener, GrpcMessagingContext context) {
      this.listener = listener;
      this.context = context;
    }

    public void accept(T event) {
      if (context != null) {
        try {
          context.executor().execute(() -> {
            listener.accept(event);
          });
        } catch (RejectedExecutionException var3) {
        }
      } else {
        listener.accept(event);
      }

    }

    public void close() {
      listeners.remove(this);
    }
  }
}
