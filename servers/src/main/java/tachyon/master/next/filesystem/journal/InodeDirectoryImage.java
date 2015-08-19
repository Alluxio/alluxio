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

package tachyon.master.next.filesystem.journal;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import tachyon.master.next.filesystem.meta.Inode;
import tachyon.master.next.journal.ImageType;

public class InodeDirectoryImage extends InodeImage {
  private final List<Long> mChildrenIds;
  private final List<InodeImage> mChildrenImages;

  public InodeDirectoryImage(long creationTimeMs, long id, String name, long parentId,
      boolean isPinned, long lastModificationTimeMs, Set<Inode> children) {
    super(creationTimeMs, id, name, parentId, isPinned, lastModificationTimeMs);
    mChildrenIds = new ArrayList<Long>(children.size());
    mChildrenImages = new ArrayList<InodeImage>(children.size());
    for (Inode node : children) {
      mChildrenIds.add(node.getId());
      mChildrenImages.add(node.toImage());
    }
  }

  public List<Long> childrenIds() {
    return mChildrenIds;
  }

  public List<InodeImage> childrenImages() {
    return mChildrenImages;
  }

  @Override
  public ImageType type() {
    return ImageType.INODE_DIRECTORY;
  }
}
