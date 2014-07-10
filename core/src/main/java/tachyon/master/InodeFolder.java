/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Collections;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectWriter;

import tachyon.Constants;
import tachyon.thrift.ClientFileInfo;

/**
 * Tachyon file system's folder representation in master.
 */
public class InodeFolder extends Inode {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /**
   * Create a new InodeFile from a JsonParser and an image Json element.
   * 
   * @param parser
   *          the JsonParser to get the next element
   * @param ele
   *          the current InodeFolder's Json image element.
   * @return the constructed InodeFolder.
   * @throws IOException
   */
  static InodeFolder loadImage(JsonParser parser, ImageElement ele) throws IOException {
    long creationTimeMs = ele.getLong("creationTimeMs");
    int fileId = ele.getInt("id");
    String fileName = ele.getString("name");
    int parentId = ele.getInt("parentId");
    boolean isPinned = ele.getBoolean("pinned");
    List<Integer> childrenIds = ele.<List<Integer>> get("childrenIds");

    int numberOfChildren = childrenIds.size();
    Inode[] children = new Inode[numberOfChildren];
    for (int k = 0; k < numberOfChildren; k ++) {
      try {
        ele = parser.readValueAs(ImageElement.class);
        LOG.debug("Read Element: " + ele);
      } catch (IOException e) {
        throw e;
      }

      switch (ele.type) {
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

    InodeFolder folder = new InodeFolder(fileName, fileId, parentId, creationTimeMs);
    folder.setPinned(isPinned);
    folder.addChildren(children);
    return folder;
  }

  private Set<Inode> mChildren = new HashSet<Inode>();

  public InodeFolder(String name, int id, int parentId, long creationTimeMs) {
    super(name, id, parentId, true, creationTimeMs);
  }

  /**
   * Adds the given inode to the set of children.
   * 
   * @param child
   *          The inode to add
   */
  public synchronized void addChild(Inode child) {
    mChildren.add(child);
  }

  /**
   * Adds the given inodes to the set of children.
   * 
   * @param children
   *          The inodes to add
   */
  public synchronized void addChildren(Inode[] children) {
    for (Inode child : children) {
      addChild(child);
    }
  }

  /**
   * Generates client file info for the folder.
   * 
   * @param path
   *          The path of the folder in the filesystem
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

    return ret;
  }

  /**
   * Returns the child with the given id.
   * 
   * @param fid
   *          The id of the child
   * @return the inode with the given id, or null if there is no child with that id
   */
  public synchronized Inode getChild(int fid) {
    for (Inode child : mChildren) {
      if (child.getId() == fid) {
        return child;
      }
    }
    return null;
  }

  /**
   * Returns the child with the given name.
   * 
   * @param name
   *          The name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  public synchronized Inode getChild(String name) {
    for (Inode child : mChildren) {
      if (child.getName().equals(name)) {
        return child;
      }
    }
    return null;
  }

  /**
   * Returns the folder's children.
   * 
   * @return an unmodifiable set of the children inodes.
   */
  public synchronized Set<Inode> getChildren() {
    return Collections.unmodifiableSet(mChildren);
  }

  /**
   * Returns the ids of the children.
   * 
   * @return the ids of the children
   */
  public synchronized List<Integer> getChildrenIds() {
    List<Integer> ret = new ArrayList<Integer>(mChildren.size());
    for (Inode child : mChildren) {
      ret.add(child.getId());
    }
    return ret;
  }

  /**
   * Returns the number of children the folder has.
   * 
   * @return the number of children in the folder.
   */
  public synchronized int getNumberOfChildren() {
    return mChildren.size();
  }

  /**
   * Removes the given inode from the folder.
   * 
   * @param child
   *          The Inode to remove
   * @return true if the inode was removed, false otherwise.
   */
  public synchronized boolean removeChild(Inode child) {
    return mChildren.remove(child);
  }

  /**
   * Removes the given child from the folder.
   * 
   * @param name
   *          The name of the Inode to remove.
   * @return true if the inode was removed, false otherwise.
   */
  public synchronized boolean removeChild(String name) {
    for (Inode child : mChildren) {
      if (child.getName().equals(name)) {
        mChildren.remove(child);
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFolder(");
    sb.append(super.toString()).append(",").append(mChildren).append(")");
    return sb.toString();
  }

  /**
   * Write an image of the folder.
   * 
   * @param os
   *          The output stream to write the folder to
   */
  @Override
  public void writeImage(ObjectWriter objWriter, DataOutputStream dos) throws IOException {
    ImageElement ele =
        new ImageElement(ImageElementType.InodeFolder)
            .withParameter("creationTimeMs", getCreationTimeMs()).withParameter("id", getId())
            .withParameter("name", getName()).withParameter("parentId", getParentId())
            .withParameter("pinned", isPinned()).withParameter("childrenIds", getChildrenIds());

    writeElement(objWriter, dos, ele);

    for (Inode inode : getChildren()) {
      inode.writeImage(objWriter, dos);
    }
  }
}
