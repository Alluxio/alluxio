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

package alluxio.client.lineage;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.client.ClientContext;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.client.util.ClientTestUtils;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;

/**
 * Tests {@link AlluxioLineage}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageContext.class, LineageMasterClient.class, ClientContext.class})
public final class AlluxioLineageTest {
  private LineageContext mLineageContext;
  private LineageMasterClient mLineageMasterClient;
  private AlluxioLineage mAlluxioLineage;

  @Before
  public void before() throws Exception {
    ClientContext.getConf().set(Constants.USER_LINEAGE_ENABLED, "true");
    mLineageMasterClient = PowerMockito.mock(LineageMasterClient.class);
    mLineageContext = PowerMockito.mock(LineageContext.class);
    Mockito.when(mLineageContext.acquireMasterClient()).thenReturn(mLineageMasterClient);
    Whitebox.setInternalState(LineageContext.class, "INSTANCE", mLineageContext);
    mAlluxioLineage = AlluxioLineage.get();
    Whitebox.setInternalState(mAlluxioLineage, "mContext", mLineageContext);
  }

  @After
  public void after() {
    ClientTestUtils.resetClientContext();
  }

  @Test
  public void getInstanceTest() {
    AlluxioLineage tl = AlluxioLineage.get();
    // same as the second get
    Assert.assertEquals(tl, AlluxioLineage.get());
  }

  @Test
  public void createLineageTest() throws Exception {
    List<AlluxioURI> inputFiles = Lists.newArrayList(new AlluxioURI("input"));
    List<AlluxioURI> outputFiles = Lists.newArrayList(new AlluxioURI("output"));
    CommandLineJob job = new CommandLineJob("cmd", new JobConf("out"));
    mAlluxioLineage.createLineage(inputFiles, outputFiles, job);
    Mockito.verify(mLineageMasterClient).createLineage(Lists.newArrayList("input"),
        Lists.newArrayList("output"), job);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  @Test
  public void deleteLineageTest() throws Exception {
    DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
    mAlluxioLineage.deleteLineage(0, options);
    Mockito.verify(mLineageMasterClient).deleteLineage(0, true);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  @Test
  public void getLineageInfoList() throws Exception {
    mAlluxioLineage.getLineageInfoList();
    Mockito.verify(mLineageMasterClient).getLineageInfoList();
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }
}
