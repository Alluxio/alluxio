package tachyon.master;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
  public void writeImageTest() throws IOException {
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
    dep.writeImage(writer, dos);

    // decode the written bytes
    ImageElement decoded = mapper.readValue(os.toByteArray(), ImageElement.class);
    TypeReference<List<Integer>> intListRef = new TypeReference<List<Integer>>() {};
    TypeReference<DependencyType> depTypeRef = new TypeReference<DependencyType>() {};
    TypeReference<List<ByteBuffer>> byteListRef = new TypeReference<List<ByteBuffer>>() {};

    // test the decoded ImageElement
    // can't use equals(decoded) because ImageElement doesn't have an equals method and can have variable fields
    Assert.assertEquals(0, decoded.getInt("depID").intValue());
    Assert.assertEquals(parents, decoded.get("parentFiles", intListRef));
    Assert.assertEquals(children, decoded.get("childrenFiles", intListRef));
    Assert.assertEquals(data, decoded.get("data", byteListRef));
    Assert.assertEquals(parentDependencies, decoded.get("parentDeps", intListRef));
    Assert.assertEquals(cmd, decoded.getString("commandPrefix"));
    Assert.assertEquals("Dependency Test", decoded.getString("comment"));
    Assert.assertEquals("Tachyon Tests", decoded.getString("framework"));
    Assert.assertEquals("0.4", decoded.getString("frameworkVersion"));
    Assert.assertEquals(DependencyType.Narrow, decoded.get("depType", depTypeRef));
    Assert.assertEquals(0L, decoded.getLong("creationTimeMs").longValue());
    Assert.assertEquals(dep.getUncheckpointedChildrenFiles(), decoded.get("unCheckpointedChildrenFiles", intListRef));
  }
}
