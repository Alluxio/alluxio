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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.options.DescendantType;
import alluxio.underfs.options.ListOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.util.stream.Collectors;

public class ObjectUnderFileSystemTest {
  private static final AlluxioConfiguration CONF = Configuration.global();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.UNDERFS_EVENTUAL_CONSISTENCY_RETRY_MAX_NUM, 20),
      Configuration.modifiableGlobal());

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

  @Test
  public void testListObjectStorageDescendantTypeNone() throws Throwable {
    mObjectUFS = new MockObjectUnderFileSystem(new AlluxioURI("/"),
        UnderFileSystemConfiguration.defaults(CONF)) {
      final UfsStatus mF1Status = new UfsFileStatus("f1", "", 0L, 0L, "", "", (short) 0777, 0L);
      final UfsStatus mF2Status = new UfsFileStatus("f2", "", 1L, 0L, "", "", (short) 0777, 0L);

      @Override
      public UfsStatus getStatus(String path) throws IOException {
        if (path.equals("root/f1")) {
          return mF1Status;
        } else if (path.equals("root/f2")) {
          return mF2Status;
        }
        throw new FileNotFoundException();
      }

      @Override
      public UfsStatus[] listStatus(String path) throws IOException {
        if (path.equals("root") || path.equals("root/")) {
          return new UfsStatus[] {mF1Status, mF2Status};
        }
        return new UfsStatus[0];
      }

      @Override
      public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
        return listStatus(path);
      }

      @Override
      protected ObjectPermissions getPermissions() {
        return new ObjectPermissions("foo", "bar", (short) 0777);
      }
    };

    UfsLoadResult result = UnderFileSystemTestUtil.performListingAsyncAndGetResult(
        mObjectUFS, "root", DescendantType.NONE);
    Assert.assertEquals(1, result.getItemsCount());
    UfsStatus status = result.getItems().collect(Collectors.toList()).get(0);
    assertEquals("root", status.getName());
  }
}
