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

package tachyon.client.lineage;

import java.util.List;

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

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;

/**
 * Tests {@link TachyonLineage}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageContext.class, LineageMasterClient.class, ClientContext.class})
public final class TachyonLineageTest {
  private LineageContext mLineageContext;
  private LineageMasterClient mLineageMasterClient;
  private TachyonLineage mTachyonLineage;
  private TachyonConf mTachyonConf;

  @Before
  public void before() throws Exception {
    mTachyonConf = new TachyonConf();
    mTachyonConf.set(Constants.USER_LINEAGE_ENABLED, "true");
    PowerMockito.mockStatic( ClientContext.class);
    PowerMockito.when(ClientContext.getConf()).thenReturn(mTachyonConf);
    mLineageMasterClient = PowerMockito.mock(LineageMasterClient.class);
    mLineageContext = PowerMockito.mock(LineageContext.class);
    Mockito.when(mLineageContext.acquireMasterClient()).thenReturn(mLineageMasterClient);
    Whitebox.setInternalState(LineageContext.class, "INSTANCE", mLineageContext);
    mTachyonLineage = TachyonLineage.get();
    Whitebox.setInternalState(mTachyonLineage, "mContext", mLineageContext);
  }

  @Test
  public void getInstanceTest() {
    TachyonLineage tl = TachyonLineage.get();
    // same as the second get
    Assert.assertEquals(tl, TachyonLineage.get());
  }

  @Test
  public void createLineageTest() throws Exception {
    List<TachyonURI> inputFiles = Lists.newArrayList(new TachyonURI("input"));
    List<TachyonURI> outputFiles = Lists.newArrayList(new TachyonURI("output"));
    CommandLineJob job = new CommandLineJob("cmd", new JobConf("out"));
    mTachyonLineage.createLineage(inputFiles, outputFiles, job);
    Mockito.verify(mLineageMasterClient).createLineage(Lists.newArrayList("input"),
        Lists.newArrayList("output"), job);
    // verify client is released
    Mockito.verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }
}
