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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import tachyon.master.DependencyType;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalEntryType;

public class DependencyEntry implements JournalEntry {
  public final int mId;
  public final List<Long> mParentFiles;
  public final List<Long> mChildrenFiles;
  public final String mCommandPrefix;
  public final List<ByteBuffer> mData;
  public final String mComment;
  public final String mFramework;
  public final String mFrameworkVersion;
  public final DependencyType mDependencyType;
  public final List<Integer> mParentDependencies;
  public final List<Integer> mChildrenDependencies;
  public final long mCreationTimeMs;
  public final List<Long> mUncheckpointedFiles;
  public final Set<Long> mLostFileIds;

  public DependencyEntry(int id, List<Long> parentFiles, List<Long> childrenFiles,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, DependencyType dependencyType, List<Integer> parentDependencies,
      List<Integer> childrenDependencies, long creationTimeMs, List<Long> uncheckpointedFiles,
      Set<Long> lostFileIds) {
    mId = id;
    mParentFiles = parentFiles;
    mChildrenFiles = childrenFiles;
    mCommandPrefix = commandPrefix;
    mData = data;
    mComment = comment;
    mFramework = framework;
    mFrameworkVersion = frameworkVersion;
    mDependencyType = dependencyType;
    mParentDependencies = parentDependencies;
    mChildrenDependencies = childrenDependencies;
    mCreationTimeMs = creationTimeMs;
    mUncheckpointedFiles = uncheckpointedFiles;
    mLostFileIds = lostFileIds;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.DEPENDENCY;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(14);
    parameters.put("id", mId);
    parameters.put("parentFiles", mParentFiles);
    parameters.put("childrenFiles", mChildrenFiles);
    parameters.put("commandPrefix", mCommandPrefix);
    parameters.put("data", mData);
    parameters.put("comment", mComment);
    parameters.put("framework", mFramework);
    parameters.put("frameworkVersion", mFrameworkVersion);
    parameters.put("dependencyType", mDependencyType);
    parameters.put("parentDependencies", mParentDependencies);
    parameters.put("childrenDependencies", mChildrenDependencies);
    parameters.put("creationTimeMs", mCreationTimeMs);
    parameters.put("uncheckpointedFiles", mUncheckpointedFiles);
    parameters.put("lostFileIds", mLostFileIds);
    return parameters;
  }
}
