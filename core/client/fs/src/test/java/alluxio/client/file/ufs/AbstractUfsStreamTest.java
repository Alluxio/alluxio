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

package alluxio.client.file.ufs;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Add unit tests for streams of {@link UfsBaseFileSystem}.
 */
@RunWith(Parameterized.class)
public abstract class AbstractUfsStreamTest {
  protected static final int CHUNK_SIZE = 100;
  protected InstancedConfiguration mConf;
  protected AlluxioURI mRootUfs;
  protected FileSystem mFileSystem;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  /**
   * Runs {@link AbstractUfsStreamTest} with different configuration combinations.
   *
   * @param localDataCacheEnabled whether local data cache is enabled
   */
  public AbstractUfsStreamTest(boolean localDataCacheEnabled) {
    mConf = Configuration.copyGlobal();
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ENABLED,
        PropertyKey.USER_CLIENT_CACHE_ENABLED.formatValue(localDataCacheEnabled), Source.RUNTIME);
  }

  protected AlluxioURI getUfsPath() {
    return new AlluxioURI(mRootUfs, String.valueOf(UUID.randomUUID()), true);
  }
}
