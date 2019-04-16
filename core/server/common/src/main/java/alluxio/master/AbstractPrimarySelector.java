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

package alluxio.master;

import alluxio.resource.LockResource;
import alluxio.util.interfaces.Scoped;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Base class for implementing primary selectors.
 *
 * This class handles the synchronization logic of getting the current state, waiting for a certain
 * state, or registering a state change listener. Subclasses just need to call
 * {@link #setState(State)} when they detect a state change. The selector starts off in SECONDARY
 * state.
 */
@ThreadSafe
public abstract class AbstractPrimarySelector implements PrimarySelector {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPrimarySelector.class);

  @GuardedBy("mListeners")
  private final Set<AtomicReference<Consumer<State>>> mListeners = new HashSet<>();

  private final Lock mStateLock = new ReentrantLock();
  private final Condition mStateCond = mStateLock.newCondition();
  @GuardedBy("mStateLock")
  private State mState = State.SECONDARY;

  protected final void setState(State state) {
    try (LockResource lr = new LockResource(mStateLock)) {
      mState = state;
      mStateCond.signalAll();
      synchronized (mListeners) {
        mListeners.forEach(listener -> listener.get().accept(state));
      }
      LOG.info("Primary selector transitioning to {}", state);
    }
  }

  @Override
  public final State getState() {
    try (LockResource lr = new LockResource(mStateLock)) {
      return mState;
    }
  }

  @Override
  public final Scoped onStateChange(Consumer<State> listener) {
    // Wrap listeners in a reference of our own to guarantee uniqueness for listener references.
    AtomicReference<Consumer<State>> listenerRef = new AtomicReference<>(listener);
    synchronized (mListeners) {
      Preconditions.checkState(mListeners.add(listenerRef), "listener already exists");
    }
    return () -> {
      synchronized (mListeners) {
        mListeners.remove(listenerRef);
      }
    };
  }

  @Override
  public final void waitForState(State state) throws InterruptedException {
    try (LockResource lr = new LockResource(mStateLock)) {
      while (mState != state) {
        mStateCond.await();
      }
    }
  }
}
