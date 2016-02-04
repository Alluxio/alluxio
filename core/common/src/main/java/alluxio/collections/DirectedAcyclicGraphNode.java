/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.collections;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
    mPayload = Preconditions.checkNotNull(payload);
    mParents = Preconditions.checkNotNull(parents);
    mChildren = Preconditions.checkNotNull(children);
  }

  /**
   * A Directed Acyclic Graph (DAG) node.
   *
   * @param payload the payload of the node
   */
  public DirectedAcyclicGraphNode(T payload) {
    this(payload, Lists.<DirectedAcyclicGraphNode<T>>newArrayList(),
        Lists.<DirectedAcyclicGraphNode<T>>newArrayList());
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
   * @return the childrens of the node
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
