package tachyon.master;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DependencyTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private String mMasterValue = "localhost";
  private String mPortValue = "8080";

  @After
  public final void after() throws Exception {
    DependencyVariables.VARIABLES.clear();
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    DependencyVariables.VARIABLES.put("master", mMasterValue);
    DependencyVariables.VARIABLES.put("port", mPortValue);
  }

  @Test
  public void ParseCommandPrefixTest() {
    String cmd = "java test.jar $master:$port";
    String parsedCmd = "java test.jar localhost:8080";
    List<Integer> parents = new ArrayList<Integer>();
    List<Integer> children = new ArrayList<Integer>();
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    Collection<Integer> parentDependencies = new ArrayList<Integer>();
    Dependency dep =
        new Dependency(0, parents, children, cmd, data, "Dependency Test", "Tachyon Tests", "0.4",
            DependencyType.Narrow, parentDependencies, 0L);
    Assert.assertEquals(parsedCmd, dep.parseCommandPrefix());
  }

  @Test
  public void writeImageTest() {
    // create the dependency, output streams, and associated objects
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    ObjectMapper mapper = JsonObject.createObjectMapper();
    ObjectWriter writer = mapper.writer();

    String cmd = "java test.jar $master:$port";
    List<Integer> parents = new ArrayList<Integer>();
    List<Integer> children = new ArrayList<Integer>();
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    Collection<Integer> parentDependencies = new ArrayList<Integer>();
    Dependency dep =
        new Dependency(0, parents, children, cmd, data, "Dependency Test", "Tachyon Tests", "0.4",
            DependencyType.Narrow, parentDependencies, 0L);

    // write the image
    try {
      dep.writeImage(writer, dos);
    } catch (IOException ioe) {
      Assert.fail("Unexpected IOException: " + ioe.getMessage());
    }

    // decode the written bytes
    ImageElement decoded = null;
    try {
      decoded = mapper.readValue(os.toByteArray(), ImageElement.class);
    } catch (Exception e) {
      Assert.fail("Unexpected " + e.getClass() + ": " + e.getMessage());
    }

    // test the decoded ImageElement
    // can't use equals(decoded) because ImageElement doesn't have an equals method and can have variable fields
    Assert.assertEquals(0, (int) decoded.getInt("depID"));
    Assert.assertEquals(parents, decoded.get("parentFiles", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(children, decoded.get("childrenFiles", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(data, decoded.get("data", new TypeReference<List<ByteBuffer>>() {}));
    Assert.assertEquals(parentDependencies, decoded.get("parentDeps", new TypeReference<Collection<Integer>>() {}));
    Assert.assertEquals(cmd, decoded.getString("commandPrefix"));
    Assert.assertEquals("Dependency Test", decoded.getString("comment"));
    Assert.assertEquals("Tachyon Tests", decoded.getString("framework"));
    Assert.assertEquals("0.4", decoded.getString("frameworkVersion"));
    Assert.assertEquals(DependencyType.Narrow, decoded.get("depType", new TypeReference<DependencyType>() {}));
    Assert.assertEquals(0L, (long) decoded.getLong("creationTimeMs"));
    Assert.assertEquals(dep.getUncheckpointedChildrenFiles(), decoded.get("unCheckpointedChildrenFiles", new TypeReference<List<Integer>>() {}));
  }
}
