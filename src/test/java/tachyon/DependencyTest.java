/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.conf.MasterConf;

public class DependencyTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Test
  public void ParseCommandPrefixTest() {
    String cmd = "java test.jar -Dtachyon.master.hostname=HOSTNAME -Dtachyon.master.port=PORT " +
        "-Dtachyon.master.web.port=50000";
    String parsedCmd = "java test.jar -Dtachyon.master.hostname=" + MasterConf.get().HOSTNAME +
        " -Dtachyon.master.port=" + MasterConf.get().PORT + " -Dtachyon.master.web.port=50000";
    List<Integer> parents = new ArrayList<Integer>();
    List<Integer> children = new ArrayList<Integer>();
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    Collection<Integer> parentDependencies = new ArrayList<Integer>();
    Dependency dep = new Dependency(0, parents, children, cmd, data, "Dependency Test",
        "Tachyon Tests", "0.4", DependencyType.Narrow, parentDependencies, 0L);
    Assert.assertEquals(parsedCmd, dep.parseCommandPrefix());
  }
}
