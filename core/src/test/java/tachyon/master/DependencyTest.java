package tachyon.master;

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
}
