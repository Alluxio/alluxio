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

/**
 * A class to hold all listeners belong to given type T.
 *
 * @param <T> the listener type
 */
public class Listeners<T> implements Iterable<Listener<T>> {
  private final List<ListenerHolder> mListeners = new CopyOnWriteArrayList();

  /**
   * Empty constructor to construct {@link Listeners}.
   */
  public Listeners() {}

  /**
   * @return the current holding listeners
   */
  public int size() {
    return mListeners.size();
  }

  /**
   * Adds a new listener.
   *
   * @param listener the listener to add
   * @return listener holder of this listener
   */
  public Listener<T> add(Consumer<T> listener) {
    Preconditions.checkNotNull(listener, "listener should not be null");
    ListenerHolder holder
        = new ListenerHolder(listener, GrpcMessagingContext.currentContext());
    mListeners.add(holder);
    return holder;
  }

  /**
   * Accepts an event.
   *
   * @param event the event to accept
   * @return the listener accepted future
   */
  public CompletableFuture<Void> accept(T event) {
    List<CompletableFuture<Void>> futures = new ArrayList(mListeners.size());
    for (ListenerHolder listener : mListeners) {
      if (listener.getContext() != null) {
        futures.add(listener.getContext().execute(() -> listener.getListener().accept(event)));
      } else {
        listener.getListener().accept(event);
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Listener<T>> iterator() {
    return (Iterator) mListeners.iterator();
  }

  /**
   * The listener holder with messaging context.
   */
  public class ListenerHolder implements Listener<T> {
    private final Consumer<T> mListener;
    private final GrpcMessagingContext mContext;

    ListenerHolder(Consumer<T> listener, GrpcMessagingContext context) {
      mListener = listener;
      mContext = context;
    }

    @Override
    public void accept(T event) {
      if (mContext != null) {
        try {
          mContext.executor().execute(() -> {
            mListener.accept(event);
          });
        } catch (RejectedExecutionException var3) {
          // Ignore the rejected exception
        }
      } else {
        mListener.accept(event);
      }
    }

    @Override
    public void close() {
      mListeners.remove(this);
    }

    private GrpcMessagingContext getContext() {
      return mContext;
    }

    private Consumer<T> getListener() {
      return mListener;
    }
  }
}
