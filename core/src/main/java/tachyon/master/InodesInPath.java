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

import java.util.NoSuchElementException;

import tachyon.TachyonURI;

/**
 * Contains Inodes information resolved from a given path.
 */
public class InodesInPath {
  /**
   * Array with the specified number of INodes
   */
  private final Inode[] mInodes;
  /**
   * Array with the specified number of pathName
   */
  private final String[] mPathByNameArr;
  private final String mFullPath;

  public InodesInPath(Inode[] inodes, String[] pathByNameArr) {
    this.mInodes = inodes;
    this.mPathByNameArr = pathByNameArr;
    this.mFullPath = constructPath(pathByNameArr);
  }

  public Inode[] getInodes() {
    return mInodes;
  }

  public String[] getPathByNameArr() {
    return mPathByNameArr;
  }

  public String getFullPath() {
    return mFullPath;
  }

  /**
   * @return the i-th inode if i >= 0; otherwise, i < 0, return the (length + i)-th inode.
   */
  public Inode getInode(int i) {
    if (mInodes == null || mInodes.length == 0) {
      throw new NoSuchElementException("inodes is null or empty");
    }
    int index = i >= 0 ? i : mInodes.length + i;
    if (index < mInodes.length && index >= 0) {
      return mInodes[index];
    } else {
      throw new NoSuchElementException("inodes.length == " + mInodes.length);
    }
  }

  public Inode getLastINode() {
    return getInode(-1);
  }

  /**
   * Given a array of pathNames returns a full path String
   */
  private String constructPath(String[] pathByNameArr) {
    if (pathByNameArr.length == 0) {
      return "";
    }
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < pathByNameArr.length; i++) {
      result.append(pathByNameArr[i]);
      if (i < pathByNameArr.length - 1) {
        result.append(TachyonURI.SEPARATOR);
      }
    }
    return result.toString();
  }
}
