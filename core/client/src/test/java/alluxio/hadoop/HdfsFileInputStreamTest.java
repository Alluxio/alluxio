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

package alluxio.hadoop;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Tests for the {@link HdfsFileInputStream} class.
 */
public class HdfsFileInputStreamTest {

  /**
   * Tests skip, verify that it correctly called {{@link HdfsFileInputStream#seek(long)}, when
   * the skipBytes is negative, assert that it returns zero.
   */
  @Test
  public void skip() throws IOException {
    HdfsFileInputStream hdfsFileInputStream = Mockito.mock(HdfsFileInputStream.class);
    Mockito.when(hdfsFileInputStream.available()).thenReturn(1000);

    Mockito.doCallRealMethod().when(hdfsFileInputStream).skip(111L);
    hdfsFileInputStream.skip(111L);
    //0 means mCurrentPostion, need to find a better way.
    Mockito.verify(hdfsFileInputStream).seek(0 + 111L);

    Mockito.doCallRealMethod().when(hdfsFileInputStream).skip(1111L);
    hdfsFileInputStream.skip(1111L);
    Mockito.verify(hdfsFileInputStream).seek(0 + 1000L);

    Mockito.doCallRealMethod().when(hdfsFileInputStream).skip(-1L);
    long toSkip3 = hdfsFileInputStream.skip(-1);
    Assert.assertEquals(0L, toSkip3);
  }
}
