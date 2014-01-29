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
  private Set<Integer> mChildren;

  public InodeFolder(String name, int id, int parentId, long creationTimeMs) {
    this(name, id, parentId, InodeType.Folder, creationTimeMs);
  }

  public InodeFolder(String name, int id, int parentId, InodeType type, long creationTimeMs) {
    super(name, id, parentId, type, creationTimeMs);
    mChildren = new HashSet<Integer>();
  }

  public synchronized void addChild(int childId) {
    mChildren.add(childId);
  }

  public synchronized void addChildren(int [] childrenIds) {
    for (int k = 0; k < childrenIds.length; k ++) {
      addChild(childrenIds[k]);
    }
  }

  public synchronized Inode getChild(String name, Map<Integer, Inode> allInodes) {
    Inode tInode = null;
    for (int childId : mChildren) {
      tInode = allInodes.get(childId);
      if (tInode != null && tInode.getName().equals(name)) {
        return tInode;
      }
    }
    return null;
  }

  public synchronized List<Integer> getChildrenIds() {
    List<Integer> ret = new ArrayList<Integer>(mChildren.size());
    ret.addAll(mChildren);
    return ret;
  }

  public synchronized int getNumberOfChildren() {
    return mChildren.size();
  }

  public synchronized void removeChild(int id) {
    mChildren.remove(id);
  }

  public synchronized boolean removeChild(String name, Map<Integer, Inode> allInodes) {
    Inode tInode = null;
    for (int childId : mChildren) {
      tInode = allInodes.get(childId);
      if (tInode != null && tInode.getName().equals(name)) {
        mChildren.remove(childId);
        return true;
      }
    }
    return false;
  }

  public boolean isRawTable() {
    return TYPE == InodeType.RawTable;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFolder(");
    sb.append(super.toString()).append(",").append(mChildren).append(")");
    return sb.toString();
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
}