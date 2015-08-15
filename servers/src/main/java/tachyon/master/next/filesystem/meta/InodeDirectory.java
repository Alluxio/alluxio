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

package tachyon.master.next.filesystem.meta;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableSet;

import tachyon.Constants;
import tachyon.master.next.ImageEntry;
import tachyon.master.next.ImageEntryType;
import tachyon.thrift.FileInfo;

/**
 * Tachyon file system's folder representation in master.
 */
public class InodeDirectory extends Inode {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static InodeDirectory load(JsonParser parser, ImageEntry entry) throws IOException {
    final long creationTimeMs = entry.getLong("creationTimeMs");
    final int fileId = entry.getInt("id");
    final boolean isPinned = entry.getBoolean("pinned");
    final String fileName = entry.getString("name");
    final int parentId = entry.getInt("parentId");
    List<Integer> childrenIds = entry.get("childrenIds", new TypeReference<List<Integer>>() {});
    final long lastModificationTimeMs = entry.getLong("lastModificationTimeMs");
    int numberOfChildren = childrenIds.size();
    Inode[] children = new Inode[numberOfChildren];
    for (int k = 0; k < numberOfChildren; k ++) {
      try {
        entry = parser.readValueAs(ImageEntry.class);
        LOG.debug("Read Element: {}", entry);
      } catch (IOException e) {
        throw e;
      }

      switch (entry.type()) {
        case InodeFile: {
          children[k] = InodeFile.load(entry);
          break;
        }
        case InodeDirectory: {
          children[k] = InodeDirectory.load(parser, entry);
          break;
        }
        default:
          throw new IOException("Invalid entryment type " + entry);
      }
    }

    InodeDirectory dir = new InodeDirectory(fileName, fileId, parentId, creationTimeMs);
    dir.setPinned(isPinned);
    dir.addChildren(children);
    dir.setLastModificationTimeMs(lastModificationTimeMs);
    return dir;
  }

  private Map<Long, Inode> mChildrenIds = new HashMap<Long, Inode>();
  private Map<String, Inode> mChildrenNames = new HashMap<String, Inode>();

  /**
   * Create a new InodeFolder.
   *
   * @param name The name of the folder
   * @param id The inode id of the folder
   * @param parentId The inode id of the parent of the folder
   * @param creationTimeMs The creation time of the folder, in milliseconds
   */
  public InodeDirectory(String name, long id, long parentId, long creationTimeMs) {
    super(name, id, parentId, true, creationTimeMs);
  }

  /**
   * Adds the given inode to the set of children.
   *
   * @param child The inode to add
   */
  public synchronized void addChild(Inode child) {
    mChildrenIds.put(child.getId(), child);
    mChildrenNames.put(child.getName(), child);
  }

  /**
   * Adds the given inodes to the set of children.
   *
   * @param children The inodes to add
   */
  public synchronized void addChildren(Inode[] children) {
    for (Inode child : children) {
      addChild(child);
    }
  }

  /**
   * Generates client file info for the folder.
   *
   * @param path The path of the folder in the filesystem
   * @return the generated FileInfo
   */
  @Override
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();

    ret.fileId = getId();
    ret.name = getName();
    ret.path = path;
    ret.ufsPath = "";
    ret.length = 0;
    ret.blockSizeByte = 0;
    ret.creationTimeMs = getCreationTimeMs();
    ret.isComplete = true;
    ret.isFolder = true;
    ret.isPinned = isPinned();
    ret.isCache = false;
    ret.blockIds = null;
    ret.dependencyId = -1;
    ret.lastModificationTimeMs = getLastModificationTimeMs();

    return ret;
  }

  /**
   * Returns the child with the given inode id.
   *
   * @param id The inode id of the child
   * @return the inode with the given id, or null if there is no child with that id
   */
  public synchronized Inode getChild(int id) {
    return mChildrenIds.get(id);
  }

  /**
   * Returns the child with the given name.
   *
   * @param name The name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  public synchronized Inode getChild(String name) {
    return mChildrenNames.get(name);
  }

  /**
   * Returns the folder's children.
   *
   * @return an unmodifiable set of the children inodes.
   */
  public synchronized Set<Inode> getChildren() {
    return ImmutableSet.copyOf(mChildrenIds.values());
  }

  /**
   * Returns the ids of the children.
   *
   * @return the ids of the children
   */
  public synchronized List<Long> getChildrenIds() {
    return new ArrayList<Long>(mChildrenIds.keySet());
  }

  /**
   * Returns the number of children the folder has.
   *
   * @return the number of children in the folder.
   */
  public synchronized int getNumberOfChildren() {
    return mChildrenIds.size();
  }

  /**
   * Removes the given inode from the folder.
   *
   * @param child The Inode to remove
   * @return true if the inode was removed, false otherwise.
   */
  public synchronized boolean removeChild(Inode child) {
    return (mChildrenIds.remove(child.getId()) != null)
        && (mChildrenNames.remove(child.getName()) != null);
  }

  /**
   * Removes the given child by its name from the folder.
   *
   * @param name The name of the Inode to remove.
   * @return true if the inode was removed, false otherwise.
   */
  public synchronized boolean removeChild(String name) {
    Inode toRemove = mChildrenNames.remove(name);
    if (toRemove != null && mChildrenIds.remove(toRemove.getId()) != null) {
      return true;
    }
    return false;
  }

  @Override
  public void dump(ObjectWriter objWriter, DataOutputStream dos) throws IOException {
    ImageEntry entry =
        new ImageEntry(ImageEntryType.InodeDirectory)
            .withParameter("creationTimeMs", getCreationTimeMs())
            .withParameter("id", getId())
            .withParameter("name", getName())
            .withParameter("parentId", getParentId())
            .withParameter("pinned", isPinned())
            .withParameter("childrenIds", getChildrenIds())
            .withParameter("lastModificationTimeMs", getLastModificationTimeMs());

    entry.dump(objWriter, dos);

    for (Inode inode : getChildren()) {
      inode.dump(objWriter, dos);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFolder(");
    sb.append(super.toString()).append(",").append(mChildrenIds.values()).append(")");
    return sb.toString();
  }
}
