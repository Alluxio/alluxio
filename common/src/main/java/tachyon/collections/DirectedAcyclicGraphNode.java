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

package tachyon.collections;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A Directed Acyclic Graph (DAG) node.
 *
 * @param <T> payload
 */
public class DirectedAcyclicGraphNode<T> {
  private final T mPayload;
  private final List<DirectedAcyclicGraphNode<T>> mParents;
  private final List<DirectedAcyclicGraphNode<T>> mChildren;

  public DirectedAcyclicGraphNode(T payload, List<DirectedAcyclicGraphNode<T>> parents,
      List<DirectedAcyclicGraphNode<T>> children) {
    mPayload = Preconditions.checkNotNull(payload);
    mParents = Preconditions.checkNotNull(parents);
    mChildren = Preconditions.checkNotNull(children);
  }

  public DirectedAcyclicGraphNode(T payload) {
    this(payload, Lists.<DirectedAcyclicGraphNode<T>>newArrayList(),
        Lists.<DirectedAcyclicGraphNode<T>>newArrayList());
  }

  public T getPayload() {
    return mPayload;
  }

  public List<DirectedAcyclicGraphNode<T>> getParents() {
    return mParents;
  }

  public List<DirectedAcyclicGraphNode<T>> getChildren() {
    return mChildren;
  }

  public void addParent(DirectedAcyclicGraphNode<T> parent) {
    mParents.add(parent);
  }

  public void addChild(DirectedAcyclicGraphNode<T> child) {
    mChildren.add(child);
  }

  public void removeChild(DirectedAcyclicGraphNode<T> child) {
    Preconditions.checkState(mChildren.contains(child));
    mChildren.remove(child);
  }
}
