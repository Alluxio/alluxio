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

package alluxio.underfs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;

public class ObjectUnderFileSystemTest {
  private static final AlluxioConfiguration CONF = Configuration.global();

  private ObjectUnderFileSystem mObjectUFS = new MockObjectUnderFileSystem(new AlluxioURI("/"),
      UnderFileSystemConfiguration.defaults(CONF));

  @Test
  public void testRetryOnException() {
    //if ufs throws SocketException, it will retry.
    ObjectUnderFileSystem.ObjectStoreOperation<Object> mockOp = Mockito.mock(
        ObjectUnderFileSystem.ObjectStoreOperation.class);
    try {
      Mockito.when(mockOp.apply()).thenThrow(new SocketException()).thenReturn(null);
      mObjectUFS.retryOnException(mockOp, () -> "test");
      // retry policy will retry, so op will apply twice.
      Mockito.verify(mockOp, Mockito.atLeast(2)).apply();
    } catch (IOException e) {
      fail();
    }

    //if ufs throws other Exception, it will throw Exception.
    try {
      Mockito.reset(mockOp);
      Mockito.when(mockOp.apply()).thenThrow(new FileNotFoundException()).thenReturn(null);
      mObjectUFS.retryOnException(mockOp, () -> "test");
    } catch (IOException e) {
      assertTrue(e instanceof FileNotFoundException);
    }
    try {
      // op only apply once and then throw exception.
      Mockito.verify(mockOp, Mockito.atLeast(1)).apply();
    } catch (IOException e) {
      fail();
    }
  }
}
