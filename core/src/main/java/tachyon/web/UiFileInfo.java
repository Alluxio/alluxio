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
package tachyon.web;

import tachyon.Constants;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

import java.util.ArrayList;
import java.util.List;

public class UiFileInfo implements Comparable<UiFileInfo> {
  private final int ID;
  private final int DEPENDENCY_ID;
  private final String NAME;
  private final String ABSOLUATE_PATH;
  private final String CHECKPOINT_PATH;
  private final long BLOCK_SIZE_BYTES;
  private final long SIZE;
  private final long CREATION_TIME_MS;
  private final boolean IN_MEMORY;
  private final int IN_MEMORY_PERCENTAGE;
  private final boolean IS_DIRECTORY;
  private final boolean NEED_PIN;
  private List<String> mFileLocations;

  public UiFileInfo(ClientFileInfo fileInfo) {
    ID = fileInfo.getId();
    DEPENDENCY_ID = fileInfo.getDependencyId();
    NAME = fileInfo.getName();
    ABSOLUATE_PATH = fileInfo.getPath();
    CHECKPOINT_PATH = fileInfo.getUfsPath();
    BLOCK_SIZE_BYTES = fileInfo.getBlockSizeByte();
    SIZE = fileInfo.getLength();
    CREATION_TIME_MS = fileInfo.getCreationTimeMs();
    IN_MEMORY = (100 == fileInfo.inMemoryPercentage);
    IN_MEMORY_PERCENTAGE = fileInfo.getInMemoryPercentage();
    IS_DIRECTORY = fileInfo.isFolder;
    NEED_PIN = fileInfo.isPinned;
    mFileLocations = new ArrayList<String>();
  }

  @Override
  public int compareTo(UiFileInfo o) {
    return ABSOLUATE_PATH.compareTo(o.getAbsolutePath());
  }

  public String getAbsolutePath() {
    return ABSOLUATE_PATH;
  }

  public String getBlockSizeBytes() {
    if (IS_DIRECTORY) {
      return " ";
    } else {
      return CommonUtils.getSizeFromBytes(BLOCK_SIZE_BYTES);
    }
  }

  public String getCheckpointPath() {
    return CHECKPOINT_PATH;
  }

  public String getCreationTime() {
    return CommonUtils.convertMsToDate(CREATION_TIME_MS);
  }

  public int getDependencyId() {
    return DEPENDENCY_ID;
  }

  public List<String> getFileLocations() {
    return mFileLocations;
  }

  public int getId() {
    return ID;
  }

  public boolean getInMemory() {
    return IN_MEMORY;
  }

  public int getInMemoryPercentage() {
    return IN_MEMORY_PERCENTAGE;
  }

  public boolean getIsDirectory() {
    return IS_DIRECTORY;
  }

  public boolean getNeedPin() {
    return NEED_PIN;
  }

  public String getName() {
    if (ABSOLUATE_PATH.equals(Constants.PATH_SEPARATOR)) {
      return "root";
    } else {
      return NAME;
    }
  }

  public String getSize() {
    if (IS_DIRECTORY) {
      return " ";
    } else {
      return CommonUtils.getSizeFromBytes(SIZE);
    }
  }

  public void setFileLocations(List<NetAddress> fileLocations) {
    for (NetAddress addr : fileLocations) {
      mFileLocations.add(new String(addr.getMHost() + ":" + addr.getMPort()));
    }
  }
}