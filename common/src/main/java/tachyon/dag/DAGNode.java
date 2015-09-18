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

package tachyon.dag;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A DAG node.
 *
 * @param <T> payload.
 */
public class DAGNode<T> {
  private final T mPayload;
  private final List<DAGNode<T>> mParents;
  private final List<DAGNode<T>> mChildren;

  public DAGNode(T payload, List<DAGNode<T>> parents, List<DAGNode<T>> children) {
    mPayload = Preconditions.checkNotNull(payload);
    mParents = Preconditions.checkNotNull(parents);
    mChildren = Preconditions.checkNotNull(children);
  }

  public DAGNode(T payload) {
    this(payload, Lists.<DAGNode<T>>newArrayList(), Lists.<DAGNode<T>>newArrayList());
  }

  public T getPayload() {
    return mPayload;
  }

  public List<DAGNode<T>> getParents() {
    return mParents;
  }

  public List<DAGNode<T>> getChildren() {
    return mChildren;
  }

  public void addParent(DAGNode<T> parent) {
    mParents.add(parent);
  }

  public void addChild(DAGNode<T> child) {
    mChildren.add(child);
  }

  public void removeChild(DAGNode<T> child) {
    Preconditions.checkState(mChildren.contains(child));
    mChildren.remove(child);
  }
}
