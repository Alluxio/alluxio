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

package tachyon.web;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;

import tachyon.TachyonURI;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

public final class UiFileInfo {
  /**
   * Provides ordering of {@link tachyon.web.UiFileInfo} based off a string comparison of the
   * absolute paths.
   */
  public static final Ordering<UiFileInfo> PATH_STRING_COMPARE = Ordering.natural().onResultOf(
      new Function<UiFileInfo, Comparable<String>>() {
        @Override
        public Comparable<String> apply(UiFileInfo input) {
          return input.mAbsolutePath;
        }
      });

  private final int mId;
  private final int mDependencyId;
  private final String mName;
  private final String mAbsolutePath;
  private final String mCheckpointPath;
  private final long mBlockSizeBytes;
  private final long mSize;
  private final long mCreationTimeMs;
  private final long mLastModificationTimeMs;
  private final boolean mInMemory;
  private final int mInMemoryPercent;
  private final boolean mIsDirectory;
  private final boolean mIsPinned;
  private List<String> mFileLocations;

  public UiFileInfo(ClientFileInfo fileInfo) {
    mId = fileInfo.getId();
    mDependencyId = fileInfo.getDependencyId();
    mName = fileInfo.getName();
    mAbsolutePath = fileInfo.getPath();
    mCheckpointPath = fileInfo.getUfsPath();
    mBlockSizeBytes = fileInfo.getBlockSizeByte();
    mSize = fileInfo.getLength();
    mCreationTimeMs = fileInfo.getCreationTimeMs();
    mLastModificationTimeMs = fileInfo.getLastModificationTimeMs();
    mInMemory = (100 == fileInfo.inMemoryPercentage);
    mInMemoryPercent = fileInfo.getInMemoryPercentage();
    mIsDirectory = fileInfo.isFolder;
    mIsPinned = fileInfo.isPinned;
    mFileLocations = new ArrayList<String>();
  }

  public String getAbsolutePath() {
    return mAbsolutePath;
  }

  public String getBlockSizeBytes() {
    if (mIsDirectory) {
      return " ";
    } else {
      return CommonUtils.getSizeFromBytes(mBlockSizeBytes);
    }
  }

  public String getCheckpointPath() {
    return mCheckpointPath;
  }

  public String getCreationTime() {
    return CommonUtils.convertMsToDate(mCreationTimeMs);
  }

  public String getModificationTime() {
    return CommonUtils.convertMsToDate(mLastModificationTimeMs);
  }

  public int getDependencyId() {
    return mDependencyId;
  }

  public List<String> getFileLocations() {
    return mFileLocations;
  }

  public int getId() {
    return mId;
  }

  public boolean getInMemory() {
    return mInMemory;
  }

  public int getInMemoryPercentage() {
    return mInMemoryPercent;
  }

  public boolean getIsDirectory() {
    return mIsDirectory;
  }

  public boolean getNeedPin() {
    return mIsPinned;
  }

  public String getName() {
    if (TachyonURI.SEPARATOR.equals(mAbsolutePath)) {
      return "root";
    } else {
      return mName;
    }
  }

  public String getSize() {
    if (mIsDirectory) {
      return " ";
    } else {
      return CommonUtils.getSizeFromBytes(mSize);
    }
  }

  public void setFileLocations(List<NetAddress> fileLocations) {
    for (NetAddress addr : fileLocations) {
      mFileLocations.add(addr.getMHost() + ":" + addr.getMPort());
    }
  }
}
