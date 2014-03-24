package tachyon.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.io.Utils;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.util.CommonUtils;

public class Dependency implements ImageWriter {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /**
   * Create a new dependency from an image stream.
   * 
   * @param is
   *          the image stream
   * @return
   * @throws IOException
   */
  static Dependency loadImage(DataInputStream is) throws IOException {
    Dependency dep =
        new Dependency(is.readInt(), Utils.readIntegerList(is), Utils.readIntegerList(is),
            Utils.readString(is), Utils.readByteBufferList(is), Utils.readString(is),
            Utils.readString(is), Utils.readString(is), DependencyType.getDependencyType(is
                .readInt()), Utils.readIntegerList(is), is.readLong());
    dep.resetUncheckpointedChildrenFiles(Utils.readIntegerList(is));
    return dep;
  }

  public final int ID;

  public final long CREATION_TIME_MS;
  public final List<Integer> PARENT_FILES;
  public final List<Integer> CHILDREN_FILES;
  private final Set<Integer> UNCHECKPOINTED_CHILDREN_FILES;
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
      DependencyType type, Collection<Integer> parentDependencies, long creationTimeMs) {
    ID = id;
    CREATION_TIME_MS = creationTimeMs;

    PARENT_FILES = new ArrayList<Integer>(parents.size());
    PARENT_FILES.addAll(parents);
    CHILDREN_FILES = new ArrayList<Integer>(children.size());
    CHILDREN_FILES.addAll(children);
    UNCHECKPOINTED_CHILDREN_FILES = new HashSet<Integer>();
    UNCHECKPOINTED_CHILDREN_FILES.addAll(CHILDREN_FILES);
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

  public synchronized void addChildrenDependency(int childDependencyId) {
    for (int dependencyId : mChildrenDependencies) {
      if (dependencyId == childDependencyId) {
        return;
      }
    }
    mChildrenDependencies.add(childDependencyId);
  }

  public synchronized void addLostFile(int fileId) {
    mLostFileIds.add(fileId);
  }

  public synchronized void childCheckpointed(int childFileId) {
    UNCHECKPOINTED_CHILDREN_FILES.remove(childFileId);
    LOG.debug("Child got checkpointed " + childFileId + " : " + toString());
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

  public synchronized List<Integer> getChildrenDependency() {
    List<Integer> ret = new ArrayList<Integer>(mChildrenDependencies.size());
    ret.addAll(mChildrenDependencies);
    return ret;
  }

  public synchronized String getCommand() {
    // TODO We should support different types of command in the future.
    // For now, assume there is only one command model.
    StringBuilder sb = new StringBuilder(parseCommandPrefix());
    sb.append(" ").append(ID);
    for (int k = 0; k < CHILDREN_FILES.size(); k ++) {
      int id = CHILDREN_FILES.get(k);
      if (mLostFileIds.contains(id)) {
        sb.append(" ").append(k);
      }
    }
    mLostFileIds.clear();
    return sb.toString();
  }

  public synchronized List<Integer> getLostFiles() {
    List<Integer> ret = new ArrayList<Integer>();
    ret.addAll(mLostFileIds);
    return ret;
  }

  synchronized List<Integer> getUncheckpointedChildrenFiles() {
    List<Integer> ret = new ArrayList<Integer>(UNCHECKPOINTED_CHILDREN_FILES.size());
    ret.addAll(UNCHECKPOINTED_CHILDREN_FILES);
    return ret;
  }

  public synchronized boolean hasCheckpointed() {
    return UNCHECKPOINTED_CHILDREN_FILES.size() == 0;
  }

  public synchronized boolean hasChildrenDependency() {
    return !mChildrenDependencies.isEmpty();
  }

  public synchronized boolean hasLostFile() {
    return !mLostFileIds.isEmpty();
  }

  String parseCommandPrefix() {
    String rtn = COMMAND_PREFIX;
    for (String s : DependencyVariables.sVariables.keySet()) {
      rtn = rtn.replace("$" + s, DependencyVariables.sVariables.get(s));
    }
    return rtn;
  }

  synchronized void resetUncheckpointedChildrenFiles(
      Collection<Integer> uncheckpointedChildrenFiles) {
    UNCHECKPOINTED_CHILDREN_FILES.clear();
    UNCHECKPOINTED_CHILDREN_FILES.addAll(uncheckpointedChildrenFiles);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Dependency[");
    sb.append("ID:").append(ID).append(", CREATION_TIME_MS:").append(CREATION_TIME_MS);
    sb.append(", Parents:").append(PARENT_FILES).append(", Children:").append(CHILDREN_FILES);
    sb.append(", COMMAND_PREFIX:").append(COMMAND_PREFIX);
    sb.append(", PARSED_COMMAND_PREFIX:").append(parseCommandPrefix());
    sb.append(", COMMENT:").append(COMMENT);
    sb.append(", FRAMEWORK:").append(FRAMEWORK);
    sb.append(", FRAMEWORK_VERSION:").append(FRAMEWORK_VERSION);
    sb.append(", PARENT_DEPENDENCIES:").append(PARENT_DEPENDENCIES);
    sb.append(", ChildrenDependencies:").append(mChildrenDependencies);
    sb.append(", UncheckpointedChildrenFiles:").append(UNCHECKPOINTED_CHILDREN_FILES);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public synchronized void writeImage(DataOutputStream os) throws IOException {
    os.writeByte(Image.T_DEPENDENCY);
    os.writeInt(ID);
    Utils.writeIntegerList(PARENT_FILES, os);
    Utils.writeIntegerList(CHILDREN_FILES, os);
    Utils.writeString(COMMAND_PREFIX, os);
    Utils.writeByteBufferList(DATA, os);
    Utils.writeString(COMMENT, os);
    Utils.writeString(FRAMEWORK, os);
    Utils.writeString(FRAMEWORK_VERSION, os);
    os.writeInt(TYPE.getValue());
    Utils.writeIntegerList(PARENT_DEPENDENCIES, os);
    os.writeLong(CREATION_TIME_MS);
    Utils.writeIntegerList(getUncheckpointedChildrenFiles(), os);
  }
}