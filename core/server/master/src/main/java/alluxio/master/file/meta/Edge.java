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

package alluxio.master.file.meta;

import com.google.common.base.Objects;

/**
 * An edge in the inode tree. Edges point from parent inode ids to child inode names.
 */
public final class Edge {
  private final long mId;
  private final String mName;

  /**
   * @param id parent inode id
   * @param name child name
   */
  public Edge(long id, String name) {
    mId = id;
    mName = name;
  }

  /**
   * @return the parent inode id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the child name
   */
  public String getName() {
    return mName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Edge)) {
      return false;
    }
    Edge that = (Edge) o;
    return Objects.equal(mId, that.mId)
        && Objects.equal(mName, that.mName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mName);
  }

  @Override
  public String toString() {
    return mId + "->" + mName;
  }
}
