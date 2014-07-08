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
package tachyon.hadoop;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import tachyon.Constants;
import tachyon.client.TachyonFS;

/**
 * Unit tests for TFS
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TachyonFS.class)
public class TFSTest {

  private TFS mTfs;

  private void mockTachyonFSGet() throws IOException {
    mockStatic(TachyonFS.class);
    TachyonFS tachyonFS = mock(TachyonFS.class);
    when(TachyonFS.get(anyString())).thenReturn(tachyonFS);
  }

  @Before
  public void setup() throws Exception {
    mTfs = new TFS();
  }

  @Test
  public void shouldInitializeWithTachyonFTSchemePassedByUser() throws Exception {
    mockTachyonFSGet();
    // when
    mTfs.initialize(new URI(Constants.HEADER_FT + "stanley:19998/tmp/path.txt"),
        new Configuration());
    // then
    verifyStatic();
    TachyonFS.get(Constants.HEADER_FT + "stanley:19998");
  }

  @Test
  public void shouldInitializeWithTachyonSchemePassedByUser() throws Exception {
    mockTachyonFSGet();
    // when
    mTfs.initialize(new URI(Constants.HEADER + "stanley:19998/tmp/path.txt"), new Configuration());
    // then
    PowerMockito.verifyStatic();
    TachyonFS.get(Constants.HEADER + "stanley:19998");
  }
}
