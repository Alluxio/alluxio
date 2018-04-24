/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.lineage;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.client.util.ClientTestUtils;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

/**
 * Tests {@link AlluxioLineage}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageContext.class, LineageMasterClient.class})
public final class AlluxioLineageTest {
  private LineageContext mLineageContext;
  private LineageMasterClient mLineageMasterClient;
  private AlluxioLineage mAlluxioLineage;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(PropertyKey.USER_LINEAGE_ENABLED, "true");

  @Before
  public void before() throws Exception {
    mLineageMasterClient = PowerMockito.mock(LineageMasterClient.class);
    mLineageContext = PowerMockito.mock(LineageContext.class);
    when(mLineageContext.acquireMasterClient()).thenReturn(mLineageMasterClient);
    mAlluxioLineage = AlluxioLineage.get(mLineageContext);
  }

  @After
  public void after() {
    ClientTestUtils.resetClient();
  }

  @Test
  public void createLineage() throws Exception {
    List<AlluxioURI> inputFiles = Lists.newArrayList(new AlluxioURI("input"));
    List<AlluxioURI> outputFiles = Lists.newArrayList(new AlluxioURI("output"));
    CommandLineJob job = new CommandLineJob("cmd", new JobConf("out"));
    mAlluxioLineage.createLineage(inputFiles, outputFiles, job);
    verify(mLineageMasterClient).createLineage(Lists.newArrayList("input"),
        Lists.newArrayList("output"), job);
    // verify client is released
    verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  @Test
  public void deleteLineage() throws Exception {
    DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
    mAlluxioLineage.deleteLineage(0, options);
    verify(mLineageMasterClient).deleteLineage(0, true);
    // verify client is released
    verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }

  @Test
  public void getLineageInfoList() throws Exception {
    mAlluxioLineage.getLineageInfoList();
    verify(mLineageMasterClient).getLineageInfoList();
    // verify client is released
    verify(mLineageContext).releaseMasterClient(mLineageMasterClient);
  }
}
