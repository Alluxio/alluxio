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

package alluxio.perf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import alluxio.perf.SlaveStatus;
import alluxio.perf.thrift.SlaveAlreadyRegisterException;
import alluxio.perf.thrift.SlaveNotRegisterException;

public class SlaveStatusTest {
  private Set<String> mSlaves;

  @Before
  public final void before() throws IOException {
    mSlaves = new HashSet<String>();
    mSlaves.add("xxx");
    mSlaves.add("yyy");
    mSlaves.add("zzz");
  }

  @Test(expected = SlaveNotRegisterException.class)
  public void allReadyNotRegisterTest() throws SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    ss.allReady("xxx");
  }

  @Test(expected = SlaveNotRegisterException.class)
  public void slaveFinishNotRegisterTest() throws SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    ss.slaveFinish("yyy", true);
  }

  @Test(expected = SlaveNotRegisterException.class)
  public void slaveReadyNotRegisterTest() throws SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    ss.slaveReady("zzz", true);
  }

  @Test(expected = SlaveAlreadyRegisterException.class)
  public void slaveRegisterAlreadyRegisterTest() throws SlaveAlreadyRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    Assert.assertTrue(ss.slaveRegister("xxx", null));
    ss.slaveRegister("xxx", null);
  }

  @Test
  public void allRegisteredTest() throws SlaveAlreadyRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    Assert.assertTrue(ss.slaveRegister("xxx", null));
    Assert.assertTrue(ss.slaveRegister("yyy", null));
    Assert.assertTrue(ss.slaveRegister("zzz", null));
    Assert.assertTrue(ss.allRegistered());
  }

  @Test
  public void finishedTest() throws SlaveAlreadyRegisterException, SlaveNotRegisterException {
    SlaveStatus ss = new SlaveStatus(mSlaves.size(), mSlaves);
    Assert.assertTrue(ss.slaveRegister("xxx", null));
    Assert.assertTrue(ss.slaveRegister("yyy", null));
    Assert.assertTrue(ss.slaveRegister("zzz", null));
    ss.slaveReady("xxx", true);
    ss.slaveReady("yyy", false);
    ss.slaveReady("zzz", true);
    Assert.assertEquals(0, ss.finished(false, 0));
    Assert.assertEquals(-1, ss.finished(true, 0));
    Assert.assertEquals(0, ss.finished(true, 100));
    ss.slaveFinish("xxx", true);
    ss.slaveFinish("yyy", true);
    ss.slaveFinish("zzz", false);
    Assert.assertEquals(1, ss.finished(false, 0));
    Assert.assertEquals(-1, ss.finished(true, 0));
    Assert.assertEquals(-1, ss.finished(true, 50));
    Assert.assertEquals(1, ss.finished(true, 100));
  }
}
