package tachyon;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import tachyon.thrift.ClientDependencyInfo;

public class Dependency {
  public final int ID;
  public final long CREATION_TIME_MS;

  public final List<Integer> PARENTS;
  public final List<Integer> CHILDREN;
  public final String COMMAND_PREFIX;
  public final List<ByteBuffer> DATA;

  public final String COMMENT;
  public final String FRAMEWORK;
  public final String FRAMEWORK_VERSION;

  public final DependencyType TYPE;

  public Dependency(int id, List<Integer> parents, List<Integer> children, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      DependencyType type) {
    ID = id;
    CREATION_TIME_MS = System.currentTimeMillis();

    PARENTS = new ArrayList<Integer>(parents.size());
    PARENTS.addAll(parents);
    CHILDREN = new ArrayList<Integer>(children.size());
    CHILDREN.addAll(children);
    COMMAND_PREFIX = commandPrefix;
    DATA = CommonUtils.cloneByteBufferList(data);

    COMMENT = comment;
    FRAMEWORK = framework;
    FRAMEWORK_VERSION = frameworkVersion;

    TYPE = type;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Dependency[");
    sb.append("ID:").append(ID).append(", CREATION_TIME_MS:").append(CREATION_TIME_MS);
    sb.append(", Parents:").append(PARENTS).append(", Children:").append(CHILDREN);
    sb.append(", COMMAND_PREFIX:").append(COMMAND_PREFIX);
    sb.append(", COMMENT:").append(COMMENT);
    sb.append(", FRAMEWORK:").append(FRAMEWORK);
    sb.append(", FRAMEWORK_VERSION:").append(FRAMEWORK_VERSION);
    sb.append("]");
    return sb.toString();
  }

  public String getCommand(List<Integer> recomputeList) {
    // TODO In future, we should support different types of command;
    // For now, assume there is only one command model.
    StringBuilder sb = new StringBuilder(COMMAND_PREFIX);
    sb.append(" ").append(Config.MASTER_HOSTNAME).append(":").append(Config.MASTER_PORT);
    sb.append(" ").append(ID);
    for (int k = 0; k < recomputeList.size(); k ++) {
      sb.append(" ").append(recomputeList.get(k));
    }
    return sb.toString();
  }

  public ClientDependencyInfo generateClientDependencyInfo() {
    ClientDependencyInfo ret = new ClientDependencyInfo();
    ret.id = ID;
    ret.parents = new ArrayList<Integer>(PARENTS.size());
    ret.parents.addAll(PARENTS);
    ret.children = new ArrayList<Integer>(CHILDREN.size());
    ret.children.addAll(CHILDREN);
    ret.data = CommonUtils.cloneByteBufferList(DATA);
    return ret;
  }
}