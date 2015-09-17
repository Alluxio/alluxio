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

import org.junit.After;
import org.junit.Before;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.file.meta.DependencyVariables;

public class DependencyIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private String mMasterValue = "localhost";
  private String mPortValue = "8080";
  private TachyonConf mMasterTachyonConf;

  @After
  public final void after() throws Exception {
    DependencyVariables.VARIABLES.clear();
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(10000, 8 * Constants.MB, Constants.GB);
    mLocalTachyonCluster.start();
    DependencyVariables.VARIABLES.put("master", mMasterValue);
    DependencyVariables.VARIABLES.put("port", mPortValue);
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
  }
/*
  // TODO(gene): Re-enable when lineage is implemented.
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
            DependencyType.Narrow, parentDependencies, 0L, mMasterTachyonConf);
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
            DependencyType.Narrow, parentDependencies, 0L, mMasterTachyonConf);

    // write the image
    dep.writeImage(writer, dos);

    // decode the written bytes
    ImageElement decoded = mapper.readValue(os.toByteArray(), ImageElement.class);
    TypeReference<List<Integer>> intListRef = new TypeReference<List<Integer>>() {};
    TypeReference<DependencyType> depTypeRef = new TypeReference<DependencyType>() {};
    TypeReference<List<ByteBuffer>> byteListRef = new TypeReference<List<ByteBuffer>>() {};

    // test the decoded ImageElement
    // can't use equals(decoded) because ImageElement doesn't have an equals method and can have
    // variable fields
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
    Assert.assertEquals(dep.getUncheckpointedChildrenFiles(),
        decoded.get("unCheckpointedChildrenFiles", intListRef));
  }
*/
}
