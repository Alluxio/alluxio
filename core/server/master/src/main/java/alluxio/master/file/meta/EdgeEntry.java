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

/**
 * Representation for an edge and the inode it points to.
 */
public class EdgeEntry {
  private final long mParentId;
  private final String mChildName;
  private final long mChildId;

  /**
   * @param parentId parent id
   * @param childName child name
   * @param childId child id
   */
  public EdgeEntry(long parentId, String childName, long childId) {
    mParentId = parentId;
    mChildName = childName;
    mChildId = childId;
  }

  /**
   * @return the parent id
   */
  public long getParentId() {
    return mParentId;
  }

  /**
   * @return the child name
   */
  public String getChildName() {
    return mChildName;
  }

  /**
   * @return the child id
   */
  public long getChildId() {
    return mChildId;
  }
}
