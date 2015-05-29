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

package tachyon.master;

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
import tachyon.master.permission.Acl;
import tachyon.master.permission.AclUtil;
import tachyon.thrift.ClientFileInfo;

/**
 * Tachyon file system's folder representation in master.
 */
public class InodeFolder extends Inode {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Create a new InodeFile from a JsonParser and an image Json element.
   *
   * @param parser the JsonParser to get the next element
   * @param ele the current InodeFolder's Json image element.
   * @return the constructed InodeFolder.
   * @throws IOException
   */
  static InodeFolder loadImage(JsonParser parser, ImageElement ele) throws IOException {
    final long creationTimeMs = ele.getLong("creationTimeMs");
    final int fileId = ele.getInt("id");
    final boolean isPinned = ele.getBoolean("pinned");
    final String fileName = ele.getString("name");
    final int parentId = ele.getInt("parentId");
    List<Integer> childrenIds = ele.get("childrenIds", new TypeReference<List<Integer>>() {});
    final long lastModificationTimeMs = ele.getLong("lastModificationTimeMs");
    final String owner = ele.getString("owner");
    final String group = ele.getString("group");
    final short permission = ele.getShort("permission");
    int numberOfChildren = childrenIds.size();
    Inode[] children = new Inode[numberOfChildren];
    for (int k = 0; k < numberOfChildren; k ++) {
      try {
        ele = parser.readValueAs(ImageElement.class);
        LOG.debug("Read Element: {}", ele);
      } catch (IOException e) {
        throw e;
      }

      switch (ele.mType) {
        case InodeFile: {
          children[k] = InodeFile.loadImage(ele);
          break;
        }
        case InodeFolder: {
          children[k] = InodeFolder.loadImage(parser, ele);
          break;
        }
        default:
          throw new IOException("Invalid element type " + ele);
      }
    }
    InodeFolder folder =
        new InodeFolder(fileName, fileId, parentId, creationTimeMs, AclUtil.getAcl(owner, group,
            permission));
    folder.setPinned(isPinned);
    folder.addChildren(children);
    folder.setLastModificationTimeMs(lastModificationTimeMs);
    return folder;
  }

  private Map<Integer, Inode> mChildrenIds = new HashMap<Integer, Inode>();
  private Map<String, Inode> mChildrenNames = new HashMap<String, Inode>();

  /**
   * Create a new InodeFolder.
   *
   * @param name The name of the folder
   * @param id The id of the folder
   * @param parentId The id of the parent of the folder
   * @param creationTimeMs The creation time of the folder, in milliseconds
   */
  public InodeFolder(String name, int id, int parentId, long creationTimeMs) {
    this(name, id, parentId, creationTimeMs, AclUtil.getAcl(InodeType.FOLDER));
  }

  public InodeFolder(String name, int id, int parentId, long creationTimeMs, Acl acl) {
    super(name, id, parentId, true, creationTimeMs, acl);
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
   * @return the generated ClientFileInfo
   */
  @Override
  public ClientFileInfo generateClientFileInfo(String path) {
    ClientFileInfo ret = new ClientFileInfo();

    ret.id = getId();
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
    ret.owner = mAcl.getUserName();
    ret.group = mAcl.getGroupName();
    ret.permission = mAcl.toShort();

    return ret;
  }

  /**
   * Returns the child with the given id.
   *
   * @param fid The id of the child
   * @return the inode with the given id, or null if there is no child with that id
   */
  public synchronized Inode getChild(int fid) {
    return mChildrenIds.get(fid);
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
  public synchronized List<Integer> getChildrenIds() {
    return new ArrayList<Integer>(mChildrenIds.keySet());
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
   * Removes the given child from the folder.
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
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFolder(");
    sb.append(super.toString()).append(",").append(mChildrenIds.values()).append(")");
    return sb.toString();
  }

  /**
   * Write an image of the folder.
   *
   * @param dos The output stream to write the folder to
   */
  @Override
  public void writeImage(ObjectWriter objWriter, DataOutputStream dos) throws IOException {
    ImageElement ele =
        new ImageElement(ImageElementType.InodeFolder)
            .withParameter("creationTimeMs", getCreationTimeMs()).withParameter("id", getId())
            .withParameter("name", getName()).withParameter("parentId", getParentId())
            .withParameter("pinned", isPinned()).withParameter("childrenIds", getChildrenIds())
            .withParameter("lastModificationTimeMs", getLastModificationTimeMs())
            .withParameter("owner", mAcl.getUserName()).withParameter("group", mAcl.getGroupName())
            .withParameter("permission", mAcl.toShort());

    writeElement(objWriter, dos, ele);

    for (Inode inode : getChildren()) {
      inode.writeImage(objWriter, dos);
    }
  }
}
