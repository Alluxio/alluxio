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

package tachyon.underfs;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import tachyon.client.block.BlockStoreContext;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystemCluster;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(UnderFileSystemCluster.class)
public class UnderFileSystemClusterTest {

  @Test
  public void getTest () throws IOException {
    PowerMockito.mockStatic(UnderFileSystemCluster.class);
    UnderFileSystemCluster instance = PowerMockito.mock(UnderFileSystemCluster.class);

    String baseDir = "good";
    TachyonConf tachyonConf = new TachyonConf();
    Mockito.when(UnderFileSystemCluster.getUnderFilesystemCluster(baseDir,
        tachyonConf)).thenReturn(instance);

    Mockito.when(instance.isStarted()).thenReturn(true);
    UnderFileSystemCluster cluster = UnderFileSystemCluster.get(baseDir, tachyonConf);
  }

}