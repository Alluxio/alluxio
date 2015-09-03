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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectWriter;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.io.Utils;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.util.io.BufferUtils;

/**
 * Describe the lineage between files. Used for recomputation.
 */
public class Dependency extends ImageWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Create a new dependency from a JSON Element.
   *
   * @param ele the JSON element
   * @return the loaded dependency
   * @throws IOException
   */
  static Dependency loadImage(ImageElement ele, TachyonConf tachyonConf) throws IOException {
    Dependency dep =
        new Dependency(ele.getInt("depID"), ele.get("parentFiles",
            new TypeReference<List<Integer>>() {}), ele.get("childrenFiles",
            new TypeReference<List<Integer>>() {}), ele.getString("commandPrefix"),
            ele.getByteBufferList("data"), ele.getString("comment"), ele.getString("framework"),
            ele.getString("frameworkVersion"), ele.get("dependencyType", DependencyType.class),
            ele.get("parentDeps", new TypeReference<List<Integer>>() {}),
            ele.getLong("creationTimeMs"), tachyonConf);
    dep.resetUncheckpointedChildrenFiles(ele.get("unCheckpointedChildrenFiles",
        new TypeReference<List<Integer>>() {}));

    return dep;
  }

  public final int mId;

  public final long mCreationTimeMs;
  public final List<Integer> mParentFiles;
  public final List<Integer> mChildrenFiles;
  private final Set<Integer> mUncheckpointedChildrenFiles;
  public final String mCommandPrefix;

  public final List<ByteBuffer> mData;
  public final String mComment;
  public final String mFramework;

  public final String mFrameworkVersion;

  public final DependencyType mDependencyType;
  public final List<Integer> mParentDependencies;
  private final List<Integer> mChildrenDependencies;

  private final Set<Integer> mLostFileIds;

  private final TachyonConf mTachyonConf;

  /**
   * Create a new dependency
   *
   * @param id The id of the dependency
   * @param parents The input files' id of the dependency
   * @param children The output files' id of the dependency
   * @param commandPrefix The prefix of the command used for recomputation
   * @param data The list of the data used for recomputation
   * @param comment The comment of the dependency
   * @param framework The framework of the dependency, used for recomputation
   * @param frameworkVersion The version of the framework
   * @param type The type of the dependency, DependencyType.Wide or DependencyType.Narrow
   * @param parentDependencies The id of the parents' dependencies
   * @param creationTimeMs The create time of the dependency, in milliseconds
   * @param tachyonConf The TachyonConf instance.
   */
  public Dependency(int id, List<Integer> parents, List<Integer> children, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      DependencyType type, Collection<Integer> parentDependencies, long creationTimeMs,
      TachyonConf tachyonConf) {
    mId = id;
    mCreationTimeMs = creationTimeMs;

    mParentFiles = new ArrayList<Integer>(parents.size());
    mParentFiles.addAll(parents);
    mChildrenFiles = new ArrayList<Integer>(children.size());
    mChildrenFiles.addAll(children);
    mUncheckpointedChildrenFiles = new HashSet<Integer>();
    mUncheckpointedChildrenFiles.addAll(mChildrenFiles);
    mCommandPrefix = commandPrefix;
    mData = BufferUtils.cloneByteBufferList(data);

    mComment = comment;
    mFramework = framework;
    mFrameworkVersion = frameworkVersion;

    mDependencyType = type;

    mParentDependencies = new ArrayList<Integer>(parentDependencies.size());
    mParentDependencies.addAll(parentDependencies);
    mChildrenDependencies = new ArrayList<Integer>(0);
    mLostFileIds = new HashSet<Integer>(0);
    mTachyonConf = tachyonConf;
  }

  /**
   * Add a child dependency, which means one of the children of the current dependency is a parent
   * of the added dependency.
   *
   * @param childDependencyId The id of the child dependency to be added
   */
  public synchronized void addChildrenDependency(int childDependencyId) {
    for (int dependencyId : mChildrenDependencies) {
      if (dependencyId == childDependencyId) {
        return;
      }
    }
    mChildrenDependencies.add(childDependencyId);
  }

  /**
   * A file lost. Add it to the dependency.
   *
   * @param fileId The id of the lost file
   */
  public synchronized void addLostFile(int fileId) {
    mLostFileIds.add(fileId);
  }

  /**
   * A child file has been checkpointed. Remove it from the uncheckpointed children list.
   *
   * @param childFileId The id of the checkpointed child file
   */
  public synchronized void childCheckpointed(int childFileId) {
    mUncheckpointedChildrenFiles.remove(childFileId);
    LOG.debug("Child got checkpointed {} : {}", childFileId, toString());
  }

  /**
   * Generate a ClientDependencyInfo, which is used for the thrift server.
   *
   * @return the generated ClientDependencyInfo
   */
  public ClientDependencyInfo generateClientDependencyInfo() {
    ClientDependencyInfo ret = new ClientDependencyInfo();
    ret.id = mId;
    ret.parents = new ArrayList<Integer>(mParentFiles.size());
    ret.parents.addAll(mParentFiles);
    ret.children = new ArrayList<Integer>(mChildrenFiles.size());
    ret.children.addAll(mChildrenFiles);
    ret.data = BufferUtils.cloneByteBufferList(mData);
    return ret;
  }

  /**
   * Get the children dependencies of this dependency. It will return a duplication.
   *
   * @return the duplication of the children dependencies
   */
  public synchronized List<Integer> getChildrenDependency() {
    List<Integer> ret = new ArrayList<Integer>(mChildrenDependencies.size());
    ret.addAll(mChildrenDependencies);
    return ret;
  }

  /**
   * Get the command used for the recomputation. Note that it will clear the set of lost files' id.
   *
   * @return the command used for the recomputation
   */
  public synchronized String getCommand() {
    // TODO We should support different types of command in the future.
    // For now, assume there is only one command model.
    StringBuilder sb = new StringBuilder(parseCommandPrefix());
    sb.append(" ").append(mTachyonConf.get(Constants.MASTER_ADDRESS));
    sb.append(" ").append(mId);
    for (int k = 0; k < mChildrenFiles.size(); k ++) {
      int id = mChildrenFiles.get(k);
      if (mLostFileIds.contains(id)) {
        sb.append(" ").append(k);
      }
    }
    mLostFileIds.clear();
    return sb.toString();
  }

  /**
   * Get the lost files of the dependency. It will return a duplication.
   *
   * @return the duplication of the lost files' id
   */
  public synchronized List<Integer> getLostFiles() {
    List<Integer> ret = new ArrayList<Integer>();
    ret.addAll(mLostFileIds);
    return ret;
  }

  /**
   * Get the uncheckpointed children files. It will return a duplication.
   *
   * @return the duplication of the uncheckpointed children files' id
   */
  synchronized List<Integer> getUncheckpointedChildrenFiles() {
    List<Integer> ret = new ArrayList<Integer>(mUncheckpointedChildrenFiles.size());
    ret.addAll(mUncheckpointedChildrenFiles);
    return ret;
  }

  /**
   * Return true if the dependency has checkpointed, which means all the children files are
   * checkpointed.
   *
   * @return true if all the children files are checkpointed, false otherwise
   */
  public synchronized boolean hasCheckpointed() {
    return mUncheckpointedChildrenFiles.size() == 0;
  }

  /**
   * Return true if it has children dependency.
   *
   * @return true if it has children dependency, false otherwise
   */
  public synchronized boolean hasChildrenDependency() {
    return !mChildrenDependencies.isEmpty();
  }

  /**
   * Return true if there exists lost file of the dependency.
   *
   * @return true if the dependency has lost file, false otherwise
   */
  public synchronized boolean hasLostFile() {
    return !mLostFileIds.isEmpty();
  }

  /**
   * Replace the prefix with the specified variable in DependencyVariables.
   *
   * @return the replaced command
   */
  String parseCommandPrefix() {
    String rtn = mCommandPrefix;
    for (String s : DependencyVariables.VARIABLES.keySet()) {
      rtn = rtn.replace("$" + s, DependencyVariables.VARIABLES.get(s));
    }
    return rtn;
  }

  /**
   * Reset the uncheckpointed children files with the specified input
   *
   * @param uckdChildrenFiles The new uncheckpointed children files' id
   */
  synchronized void resetUncheckpointedChildrenFiles(Collection<Integer> uckdChildrenFiles) {
    mUncheckpointedChildrenFiles.clear();
    mUncheckpointedChildrenFiles.addAll(uckdChildrenFiles);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Dependency[");
    sb.append("Id:").append(mId).append(", CreationTimeMs:").append(mCreationTimeMs);
    sb.append(", Parents:").append(mParentFiles).append(", Children:").append(mChildrenFiles);
    sb.append(", CommandPrefix:").append(mCommandPrefix);
    sb.append(", ParsedCommandPrefix:").append(parseCommandPrefix());
    sb.append(", Comment:").append(mComment);
    sb.append(", Framework:").append(mFramework);
    sb.append(", FrameworkVersion:").append(mFrameworkVersion);
    sb.append(", ParentDependencies:").append(mParentDependencies);
    sb.append(", ChildrenDependencies:").append(mChildrenDependencies);
    sb.append(", UncheckpointedChildrenFiles:").append(mUncheckpointedChildrenFiles);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public synchronized void writeImage(ObjectWriter objWriter, DataOutputStream dos)
      throws IOException {
    ImageElement ele =
        new ImageElement(ImageElementType.Dependency).withParameter("depID", mId)
            .withParameter("parentFiles", mParentFiles)
            .withParameter("childrenFiles", mChildrenFiles)
            .withParameter("commandPrefix", mCommandPrefix)
            .withParameter("data", Utils.byteBufferListToBase64(mData))
            .withParameter("comment", mComment).withParameter("framework", mFramework)
            .withParameter("frameworkVersion", mFrameworkVersion)
            .withParameter("depType", mDependencyType)
            .withParameter("parentDeps", mParentDependencies)
            .withParameter("creationTimeMs", mCreationTimeMs)
            .withParameter("unCheckpointedChildrenFiles", getUncheckpointedChildrenFiles());
    writeElement(objWriter, dos, ele);
  }
}
