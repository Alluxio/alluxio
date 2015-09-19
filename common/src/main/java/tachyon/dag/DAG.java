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
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A DAG.
 *
 * @param <T> the payload of each node.
 */
public class DAG<T> {
  private List<DAGNode<T>> mRoots;
  private Map<T, DAGNode<T>> mIndex;

  public DAG() {
    mRoots = Lists.newArrayList();
    mIndex = Maps.newHashMap();
  }

  public void add(T payload, List<T> parents) {
    Preconditions.checkState(!contains(payload), "the payload already exists in the DAG");

    // construct the new node
    DAGNode<T> newNode = new DAGNode<T>(payload);
    mIndex.put(payload, newNode);

    // find parent nodes
    for (T parent : parents) {
      Preconditions.checkState(contains(parent),
          "the parent payload " + parent + " does not exist in the DAG");
      DAGNode<T> parentNode = mIndex.get(parent);
      parentNode.addChild(newNode);
      newNode.addParent(parentNode);
    }
  }

  public void deleteLeaf(T payload) {
    Preconditions.checkState(contains(payload), "the node does not exist");
    DAGNode<T> node = mIndex.get(payload);
    Preconditions.checkState(node.getChildren().isEmpty(), "the node is not a leaf");

    // delete from parent
    for (DAGNode<T> parent : node.getParents()) {
      parent.removeChild(node);
    }

    // remove from index
    mIndex.remove(payload);
  }

  /**
   * @return true if there a node in the DAG contains the given value as payload, false otherwise.
   */
  public boolean contains(T payload) {
    return mIndex.containsKey(payload);
  }

  /**
   * @return the children's payloads, null if the given payload doesn't exist in the DAG.
   */
  public List<T> getChildren(T payload) {
    if (!mIndex.containsKey(payload)) {
      return null;
    }
    DAGNode<T> node = mIndex.get(payload);
    List<T> children = Lists.newArrayList();
    for (DAGNode<T> child : node.getChildren()) {
      children.add(child.getPayload());
    }
    return children;
  }

  /**
   * @return the parents' payloads, null if the given payload doesn't exist in the DAG.
   */
  public List<T> getParents(T payload) {
    if (!mIndex.containsKey(payload)) {
      return null;
    }
    DAGNode<T> node = mIndex.get(payload);
    List<T> parents = Lists.newArrayList();
    for (DAGNode<T> parent : node.getParents()) {
      parents.add(parent.getPayload());
    }
    return parents;
  }

  /**
   * @return true if the payload is in the root of the DAG, false otherwise.
   */
  public boolean isRoot(T payload) {
    if (!contains(payload)) {
      return false;
    }
    return mRoots.contains(mIndex.get(payload));
  }

  /**
   * @return all the root payloads.
   */
  public List<T> getRoots() {
    List<T> roots = Lists.newArrayList();
    for (DAGNode<T> root : mRoots) {
      roots.add(root.getPayload());
    }
    return roots;
  }
}
