package tachyon;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.thrift.ClientDependencyInfo;

public class Dependency {
  public final int ID;
  public final long CREATION_TIME_MS;

  public final List<Integer> PARENT_FILES;
  public final List<Integer> CHILDREN_FILES;
  private Set<Integer> mUncheckpointedChildrenFiles;
  public final String COMMAND_PREFIX;
  public final List<ByteBuffer> DATA;

  public final String COMMENT;
  public final String FRAMEWORK;
  public final String FRAMEWORK_VERSION;

  public final DependencyType TYPE;

  public final List<Integer> PARENT_DEPENDENCIES;
  private List<Integer> mChildrenDependencies;
  private Set<Integer> mLostFileIds;

  public Dependency(int id, List<Integer> parents, List<Integer> children, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      DependencyType type, Set<Integer> parentDependencies) {
    ID = id;
    CREATION_TIME_MS = System.currentTimeMillis();

    PARENT_FILES = new ArrayList<Integer>(parents.size());
    PARENT_FILES.addAll(parents);
    CHILDREN_FILES = new ArrayList<Integer>(children.size());
    CHILDREN_FILES.addAll(children);
    mUncheckpointedChildrenFiles = new HashSet<Integer>();
    mUncheckpointedChildrenFiles.addAll(CHILDREN_FILES);
    COMMAND_PREFIX = commandPrefix;
    DATA = CommonUtils.cloneByteBufferList(data);

    COMMENT = comment;
    FRAMEWORK = framework;
    FRAMEWORK_VERSION = frameworkVersion;

    TYPE = type;

    PARENT_DEPENDENCIES = new ArrayList<Integer>(parentDependencies.size());
    PARENT_DEPENDENCIES.addAll(parentDependencies);
    mChildrenDependencies = new ArrayList<Integer>(0);
    mLostFileIds = new HashSet<Integer>(0);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Dependency[");
    sb.append("ID:").append(ID).append(", CREATION_TIME_MS:").append(CREATION_TIME_MS);
    sb.append(", Parents:").append(PARENT_FILES).append(", Children:").append(CHILDREN_FILES);
    sb.append(", COMMAND_PREFIX:").append(COMMAND_PREFIX);
    sb.append(", COMMENT:").append(COMMENT);
    sb.append(", FRAMEWORK:").append(FRAMEWORK);
    sb.append(", FRAMEWORK_VERSION:").append(FRAMEWORK_VERSION);
    sb.append("]");
    return sb.toString();
  }

  public synchronized String getCommand() {
    // TODO In future, we should support different types of command;
    // For now, assume there is only one command model.
    StringBuilder sb = new StringBuilder(COMMAND_PREFIX);
    sb.append(" ").append(Config.MASTER_HOSTNAME).append(":").append(Config.MASTER_PORT);
    sb.append(" ").append(ID);
    for (int k = 0; k < PARENT_FILES.size(); k ++) {
      int id = PARENT_FILES.get(k);
      if (mLostFileIds.contains(id)) {
        sb.append(" ").append(id);
      }
    }
    mLostFileIds.clear();
    return sb.toString();
  }

  public ClientDependencyInfo generateClientDependencyInfo() {
    ClientDependencyInfo ret = new ClientDependencyInfo();
    ret.id = ID;
    ret.parents = new ArrayList<Integer>(PARENT_FILES.size());
    ret.parents.addAll(PARENT_FILES);
    ret.children = new ArrayList<Integer>(CHILDREN_FILES.size());
    ret.children.addAll(CHILDREN_FILES);
    ret.data = CommonUtils.cloneByteBufferList(DATA);
    return ret;
  }

  public synchronized void addChildrenDependency(int childDependencyId) {
    for (int dependencyId : mChildrenDependencies) {
      if (dependencyId == childDependencyId) {
        return;
      }
    }
    mChildrenDependencies.add(childDependencyId);
  }

  public synchronized List<Integer> getChildrenDependency() {
    List<Integer> ret = new ArrayList<Integer>(mChildrenDependencies.size());
    ret.addAll(mChildrenDependencies);
    return ret;
  }

  public synchronized boolean hasChildrenDependency() {
    return !mChildrenDependencies.isEmpty();
  }

  public synchronized boolean hasCheckpointed() {
    return mUncheckpointedChildrenFiles.size() == 0;
  }

  public synchronized void childCheckpointed(int childFileId) {
    mUncheckpointedChildrenFiles.remove(childFileId);
  }

  public synchronized void addLostFile(int fileId) {
    mLostFileIds.add(fileId);
  }

  public synchronized List<Integer> getLostFiles() {
    List<Integer> ret = new ArrayList<Integer>();
    ret.addAll(mLostFileIds);
    return ret;
  }

  public synchronized boolean hasLostFile() {
    return !mLostFileIds.isEmpty();
  }
}