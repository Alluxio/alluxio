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

package alluxio;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class StreamCacheTest {
  @Test
  public void operations() throws Exception {
    StreamCache streamCache = new StreamCache();
    FileInStream is = Mockito.mock(FileInStream.class);
    FileOutStream os = Mockito.mock(FileOutStream.class);
    Integer isId = streamCache.put(is);
    Integer osId = streamCache.put(os);
    Assert.assertNotNull(streamCache.getInStream(isId));
    Assert.assertNull(streamCache.getInStream(osId));
    Assert.assertNull(streamCache.getOutStream(isId));
    Assert.assertNotNull(streamCache.getOutStream(osId));
    Assert.assertNotNull(streamCache.invalidate(isId));
    Assert.assertNotNull(streamCache.invalidate(osId));
    Assert.assertNull(streamCache.invalidate(isId));
    Assert.assertNull(streamCache.invalidate(osId));
    Mockito.verify(is, Mockito.times(1)).close();
    Mockito.verify(os, Mockito.times(1)).close();
  }

  @Test
  public void expiration() throws Exception {
    String oldValue = Configuration.get(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS);
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, 0);
    StreamCache streamCache = new StreamCache();
    FileInStream is = Mockito.mock(FileInStream.class);
    FileOutStream os = Mockito.mock(FileOutStream.class);
    streamCache.put(is);
    streamCache.put(os);
    Mockito.verify(is, Mockito.times(1)).close();
    Mockito.verify(os, Mockito.times(1)).close();
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, oldValue);
  }
}
