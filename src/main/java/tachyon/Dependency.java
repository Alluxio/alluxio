package tachyon;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
    DATA = new ArrayList<ByteBuffer>(data.size());
    for (int k = 0; k < data.size(); k ++) {
      ByteBuffer tBuf = ByteBuffer.allocate(data.get(k).limit());
      tBuf.put(data.get(k));
      tBuf.flip();
      DATA.add(tBuf);
    }

    COMMENT = comment;
    FRAMEWORK = framework;
    FRAMEWORK_VERSION = frameworkVersion;

    TYPE = type;
  }
}