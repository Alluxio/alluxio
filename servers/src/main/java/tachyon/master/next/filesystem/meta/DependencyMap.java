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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import tachyon.Constants;

/**
 * This class maintains the dependency related metadata information for the lineage feature.
 */
public class DependencyMap {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Map<Integer, Dependency> mDependencyMap;
  private final Map<Long, Dependency> mFileIdToDependency;

  private final Set<Integer> mUncheckpointedDependencies;
  private final Set<Integer> mPriorityDependencies;
  private final Set<Long> mLostFiles;
  private final Set<Long> mBeingRecomputedFiles;
  private final Set<Integer> mToRecompute;

  public DependencyMap() {
    mDependencyMap = new HashMap<Integer, Dependency>();
    mFileIdToDependency = new HashMap<Long, Dependency>();

    mUncheckpointedDependencies = new HashSet<Integer>();
    mPriorityDependencies = new HashSet<Integer>();
    mLostFiles = new HashSet<Long>();
    mBeingRecomputedFiles = new HashSet<Long>();
    mToRecompute = new HashSet<Integer>();
  }

  public Dependency getFromDependencyId(int dependencyId) {
    return mDependencyMap.get(dependencyId);
  }

  public Dependency getFromFileId(long fileId) {
    return mFileIdToDependency.get(fileId);
  }

  public void removeUncheckpointedDependency(Dependency dependency) {
    mUncheckpointedDependencies.remove(dependency.mId);
  }

  public void removePriorityDependency(Dependency dependency) {
    mPriorityDependencies.remove(dependency.mId);
  }

  public void addFileCheckpoint(long fileId) {
    if (mLostFiles.contains(fileId)) {
      mLostFiles.remove(fileId);
    }
    if (mBeingRecomputedFiles.contains(fileId)) {
      mBeingRecomputedFiles.remove(fileId);
    }
  }

  public void recomputeDependency(int dependencyId) {
    mToRecompute.add(dependencyId);
  }

  public Dependency addLostFile(long fileId) {
    mLostFiles.add(fileId);
    Dependency dependency = getFromFileId(fileId);
    if (dependency == null) {
      LOG.error("There is no dependency info for fileId: " + fileId + " . No recovery on that.");
      return null;
    }
    recomputeDependency(dependency.mId);
    return dependency;
  }

  public List<Integer> getPriorityDependencyList() {
    int earliestDependencyId = -1;
    if (mPriorityDependencies.isEmpty()) {
      long earliestCreationTime = Long.MAX_VALUE;

      for (int dependencyId : mUncheckpointedDependencies) {
        Dependency dependency = mFileIdToDependency.get(dependencyId);
        if (!dependency.hasChildrenDependency()) {
          // Add leaf dependency.
          mPriorityDependencies.add(dependency.mId);
        }
        if (dependency.mCreationTimeMs < earliestCreationTime) {
          // Keep track of earliest created dependency.
          earliestCreationTime = dependency.mCreationTimeMs;
          earliestDependencyId = dependency.mId;
        }
      }

      if (!mPriorityDependencies.isEmpty()) {
        LOG.info("New computed priority dependency list: " + mPriorityDependencies);
      } else if (earliestDependencyId != -1) {
        mPriorityDependencies.add(earliestDependencyId);
        LOG.info("Priority dependency list by earliest creation time: " + mPriorityDependencies);
      }
    }
    return Lists.newArrayList(mPriorityDependencies);
  }
}
