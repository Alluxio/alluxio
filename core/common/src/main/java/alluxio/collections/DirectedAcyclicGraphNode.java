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

package alluxio.collections;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A Directed Acyclic Graph (DAG) node.
 *
 * @param <T> payload
 */
@NotThreadSafe
public class DirectedAcyclicGraphNode<T> {
  private final T mPayload;
  private final List<DirectedAcyclicGraphNode<T>> mParents;
  private final List<DirectedAcyclicGraphNode<T>> mChildren;

  /**
   * A Directed Acyclic Graph (DAG) node.
   *
   * @param payload the payload of the node
   * @param parents the parents of the node
   * @param children the children of the node
   */
  public DirectedAcyclicGraphNode(T payload, List<DirectedAcyclicGraphNode<T>> parents,
      List<DirectedAcyclicGraphNode<T>> children) {
    mPayload = Preconditions.checkNotNull(payload, "payload");
    mParents = Preconditions.checkNotNull(parents, "parents");
    mChildren = Preconditions.checkNotNull(children, "children");
  }

  /**
   * A Directed Acyclic Graph (DAG) node.
   *
   * @param payload the payload of the node
   */
  public DirectedAcyclicGraphNode(T payload) {
    this(payload, new ArrayList<DirectedAcyclicGraphNode<T>>(),
      new ArrayList<DirectedAcyclicGraphNode<T>>());
  }

  /**
   * Gets the payload of the node.
   *
   * @return the payload of the node
   */
  public T getPayload() {
    return mPayload;
  }

  /**
   * Gets the parent nodes of the node.
   *
   * @return the parents of the node
   */
  public List<DirectedAcyclicGraphNode<T>> getParents() {
    return mParents;
  }

  /**
   * Gets the children nodes of the node.
   *
   * @return the children of the node
   */
  public List<DirectedAcyclicGraphNode<T>> getChildren() {
    return mChildren;
  }

  /**
   * Adds a parent node to the node.
   *
   * @param parent the node to be added as a parent
   */
  public void addParent(DirectedAcyclicGraphNode<T> parent) {
    mParents.add(parent);
  }

  /**
   * Adds a child node to the node.
   *
   * @param child the node to be added as a child
   */
  public void addChild(DirectedAcyclicGraphNode<T> child) {
    mChildren.add(child);
  }

  /**
   * Removes a child node from the node.
   *
   * @param child the child node to be removed
   */
  public void removeChild(DirectedAcyclicGraphNode<T> child) {
    Preconditions.checkState(mChildren.contains(child));
    mChildren.remove(child);
  }
}
