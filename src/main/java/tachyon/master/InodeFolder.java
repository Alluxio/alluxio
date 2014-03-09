/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import tachyon.thrift.ClientFileInfo;

/**
 * Tachyon file system's folder representation in master.
 */
public class InodeFolder extends Inode {
  private Set<Inode> mChildren;
  public ReadWriteLock rwl;

  public InodeFolder(String name, int id, int parentId, InodeType type, long creationTimeMs) {
    super(name, id, parentId, type, creationTimeMs);
    mChildren = new HashSet<Inode>();
    rwl = new ReadWriteLock();
  }

  public InodeFolder(String name, int id, int parentId, long creationTimeMs) {
    this(name, id, parentId, InodeType.Folder, creationTimeMs);
  }

  /* ------------------------------------------------------------------------------
   * These operations assume a lock has already been taken on the folder.
   * ------------------------------------------------------------------------------
   */

  public void addChild(Inode child) {
    mChildren.add(child);
  }

  public void addChildren(Inode[] children) {
    for (Inode i : children) {
      addChild(i);
    }
  }

  public void addChildren(int[] childrenIds, Map<Integer, Inode> allInodes) {
    for (int i : childrenIds) {
      Inode inode = allInodes.get(i);
      if (inode != null) {
        addChild(inode);
      }
    }
  }

  @Override
  public ClientFileInfo generateClientFileInfo(String path) {
    ClientFileInfo ret = new ClientFileInfo();

    ret.id = getId();
    ret.name = getName();
    ret.path = path;
    ret.checkpointPath = "";
    ret.length = 0;
    ret.blockSizeByte = 0;
    ret.creationTimeMs = getCreationTimeMs();
    ret.complete = true;
    ret.folder = true;
    ret.inMemory = true;
    ret.needPin = false;
    ret.needCache = false;
    ret.blockIds = null;
    ret.dependencyId = -1;

    return ret;
  }

  public Inode getChild(String name) {
    for (Inode i : mChildren) {
      if (i.getName().equals(name)) {
        return i;
      }
    }
    return null;
  }

  public List<Integer> getChildrenIds() {
    List<Integer> ret = new ArrayList<Integer>(mChildren.size());
    for (Inode i : mChildren) {
      ret.add(i.getId());
    }
    return ret;
  }

  // It adds nodes in parent-first order
  public List<Inode> getChildren(boolean recursive) {
    List<Inode> ret = new ArrayList<Inode>();
    if (!recursive) {
      ret.addAll(mChildren);
    } else {
      for (Inode i : mChildren) {
        ret.add(i);
        if (i.isDirectory()) {
          InodeFolder ifold = (InodeFolder) i;
          ifold.rwl.readLock();
          ret.addAll(ifold.getChildren(true));
          ifold.rwl.readUnlock();
        }
      }
    }
    return ret;
  }

  // It adds nodes in parent-first order
  public List<String> getChildrenPaths(String path, boolean recursive) {
    List<String> ret = new ArrayList<String>();
    for (Inode i : mChildren) {
      String subpath;
      if (path.endsWith("/")) {
        subpath = path + i.getName();
      } else {
        subpath = path + "/" + i.getName();
      }
      ret.add(subpath);
      if (i.isDirectory() && recursive) {
        InodeFolder ifold = (InodeFolder) i;
        ifold.rwl.readLock();
        ret.addAll(ifold.getChildrenPaths(subpath, true));
        ifold.rwl.readUnlock();
      }
    }
    return ret;
  }

  public int getNumberOfChildren() {
    return mChildren.size();
  }

  public boolean removeChild(Inode i) {
    return mChildren.remove(i);
  }

  public boolean removeChild(String name) {
    for (Inode i : mChildren) {
      if (i.getName().equals(name)) {
        mChildren.remove(i);
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
}
