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

package alluxio.node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The modular hashing node provider implementation.
 * @param <T>
 */
public class ModularHashingNodeProvider<T> implements NodeProvider {
  private List<T> mLastNodes;

  /**
   * Constructs a new {@link ModularHashingNodeProvider}.
   *
   * @param nodes the nodes to select
   */
  public ModularHashingNodeProvider(List<T> nodes) {
    mLastNodes = nodes;
  }

  @Override
  public List<T> get(Object identifier, int count) {
    if (mLastNodes == null || mLastNodes.isEmpty()) {
      return Collections.emptyList();
    }
    int size = mLastNodes.size();
    int mod = identifier.hashCode() % size;
    int position = mod < 0 ? mod + size : mod;
    List<T> chosenCandidates = new ArrayList<>();
    for (int i = 0; i < count && i < mLastNodes.size(); i++) {
      chosenCandidates.add(mLastNodes.get((position + i) % size));
    }
    return chosenCandidates;
  }

  @Override
  public void refresh(List nodes) {
    mLastNodes = nodes;
  }
}
