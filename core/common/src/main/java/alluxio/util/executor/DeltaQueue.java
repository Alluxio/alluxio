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

import javax.annotation.Nullable;

/**
 * A delay queue with extra supports for passing the head element delay time.
 *
 * @param <T> the type of the queue objects
 */
public class DeltaQueue<T> {
  private Node<T> mHead = null;

  /**
   * @return whether this queue is empty
   */
  public boolean isEmpty() {
    return mHead == null;
  }

  /**
   * @return the value of head element
   */
  public T next() {
    return mHead.getValue();
  }

  /**
   * @return the delay of head element
   */
  public long delay() {
    return mHead.mDelay;
  }

  /**
   * Gets the delay of the input element.
   *
   * @param element the target element
   * @return the total delay of the element
   */
  public long delay(T element) {
    long ret = 0;
    Node<T> next = mHead;
    while (next != null) {
      ret += next.getDelay();
      if (next.getValue().equals(element)) {
        break;
      }
      next = next.getNext();
    }
    if (next == null) {
      throw new IllegalStateException("Element not found in the delta queue");
    }
    return ret;
  }

  /**
   * Adds a new node into the queue.
   *
   * @param delay the delay in milliseconds
   * @param value the value
   */
  public void add(long delay, T value) {
    Node<T> newNode = new Node<T>(value, delay);
    Node<T> prev = null;
    Node<T> next = mHead;

    while (next != null && next.getDelay() <= newNode.getDelay()) {
      newNode.setDelay(newNode.getDelay() - next.getDelay());
      prev = next;
      next = next.getNext();
    }

    if (prev == null) {
      mHead = newNode;
    } else {
      prev.setNext(newNode);
    }

    if (next != null) {
      next.setDelay(next.getDelay() - newNode.getDelay());
      newNode.setNext(next);
    }
  }

  /**
   * Jump to a future time period.
   * The time to jump is limited by the delay of the head element.
   *
   * @param duration the time period to jump
   * @return the remaining delay to jump
   */
  public long tick(long duration) {
    if (mHead == null) {
      return 0L;
    } else if (mHead.getDelay() >= duration) {
      mHead.setDelay(mHead.getDelay() - duration);
      return 0L;
    } else {
      long leftover = duration - mHead.getDelay();
      mHead.setDelay(0L);
      return leftover;
    }
  }

  /**
   * @return the head element if it does not have delay
   */
  public T pop() {
    if (mHead.getDelay() > 0) {
      throw new IllegalStateException("cannot pop the head element when it has a non-zero delay");
    }
    T popped = mHead.getValue();
    mHead = mHead.getNext();
    return popped;
  }

  /**
   * Removes an element.
   *
   * @param element an element
   * @return true if the element removed successfully, false otherwise
   */
  public boolean remove(T element) {
    Node<T> prev = null;
    Node<T> node = mHead;
    while (node != null && node.getValue() != element) {
      prev = node;
      node = node.getNext();
    }

    if (node == null) {
      return false;
    }

    if (node.getNext() != null) {
      Node<T> nextNode = node.getNext();
      nextNode.setDelay(nextNode.getDelay() + node.getDelay());
    }

    if (prev == null) {
      mHead = node.getNext();
    } else {
      prev.setNext(node.getNext());
    }

    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");

    Node<T> node = mHead;
    while (node != null) {
      if (node != mHead) {
        sb.append(", ");
      }
      sb.append("+").append(node.getDelay()).append(": ").append(node.getValue());
      node = node.getNext();
    }
    sb.append("]");

    return sb.toString();
  }

  /**
   * A node in delta queue.
   */
  private static class Node<T> {
    private final T mValue;
    private long mDelay;
    private Node<T> mNext = null;

    /**
     * Constructs a new {@link Node}.
     *
     * @param value a value
     * @param millis delay time in milliseconds
     */
    public Node(T value, long millis) {
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
     * @return the next node
     */
    @Nullable
    public Node<T> getNext() {
      return mNext;
    }

    /**
     * Sets the delay.
     *
     * @param delay the delay in milliseconds
     */
    public void setDelay(long delay) {
      mDelay = delay;
    }

    /**
     * Sets the next element.
     *
     * @param next the next element
     */
    public void setNext(Node<T> next) {
      mNext = next;
    }
  }
}
