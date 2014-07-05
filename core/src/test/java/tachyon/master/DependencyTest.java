/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import tachyon.master.Dependency;
import tachyon.master.DependencyType;
import tachyon.master.DependencyVariables;

public class DependencyTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private String mMasterValue = "localhost";
  private String mPortValue = "8080";

  @After
  public final void after() throws Exception {
    DependencyVariables.sVariables.clear();
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    DependencyVariables.sVariables.put("master", mMasterValue);
    DependencyVariables.sVariables.put("port", mPortValue);
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
