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

package alluxio.util.executor;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A controllable queue is a priority queue with {@link DelayNode}s.
 * It supports tick the time forward and pop nodes whose delay is smaller than the past time.
 *
 * @param <T> the type of the value of {@link DelayNode}
 */
public class ControllableQueue<T> {
  private long mPastTime;
  private PriorityQueue<DelayNode<T>> mQueue;

  /**
   * Constructs a new {@link ControllableQueue}.
   */
  public ControllableQueue() {
    mPastTime = 0;
    mQueue = new PriorityQueue<>(Comparator.comparing(DelayNode::getDelay));
  }

  /**
   * @return whether this queue is empty
   */
  public boolean isEmpty() {
    return mQueue.isEmpty();
  }

  /**
   * @return the value of head element
   */
  public T getHeadValue() {
    return mQueue.peek().getValue();
  }

  /**
   * @return the delay of head element
   */
  public long getHeadDelay() {
    return mQueue.peek().getDelay() - mPastTime;
  }

  /**
   * Adds a new node into the queue.
   *
   * @param delay the delay in milliseconds
   * @param value the value
   */
  public void add(long delay, T value) {
    mQueue.add(new DelayNode<>(value, delay + mPastTime));
  }

  /**
   * Jumps to a future time period.
   *
   * @param duration the time period to jump
   */
  public void tick(long duration) {
    mPastTime += duration;
  }

  /**
   * @return the head element if it should be executed
   */
  public T pop() {
    if (getHeadDelay() > 0) {
      throw new IllegalStateException("cannot pop the head element when it has a non-zero delay");
    }
    return mQueue.poll().getValue();
  }

  /**
   * Removes an element.
   *
   * @param element an element
   * @return true if the element removed successfully, false otherwise
   */
  public boolean remove(T element) {
    return mQueue.removeIf(x -> x.getValue() == element);
  }

  @Override
  public String toString() {
    return "mPastTime=" + mPastTime + ", mQueue=" + mQueue.toString();
  }

  /**
   * A delay node in delta queue which records the value
   * and the total delay (which is the original delay plus the past time).
   *
   * @param <T> the type of the value
   */
  public static class DelayNode<T> {
    private final T mValue;
    private long mDelay;

    /**
     * Constructs a new {@link DelayNode}.
     *
     * @param value a value
     * @param millis delay time in milliseconds
     */
    public DelayNode(T value, long millis) {
      mValue = value;
      mDelay = millis;
    }

    /**
     * @return the value
     */
    public T getValue() {
      return mValue;
    }

    /**
     * @return the delay in milliseconds
     */
    public long getDelay() {
      return mDelay;
    }

    /**
     * Sets the delay.
     *
     * @param delay the delay in milliseconds
     */
    public void setDelay(long delay) {
      mDelay = delay;
    }
  }
}
