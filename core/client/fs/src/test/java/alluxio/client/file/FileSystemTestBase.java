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

package alluxio.client.file;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.ClientContext;
import alluxio.TestLoggerRule;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.CloseableResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.powermock.api.mockito.PowerMockito;

/**
 * Test base for {@link FileSystem} related test.
 */
public class FileSystemTestBase {

  protected static final RuntimeException EXCEPTION = new RuntimeException("test exception");
  protected static final String SHOULD_HAVE_PROPAGATED_MESSAGE =
      "Exception should have been propagated";

  protected InstancedConfiguration mConf = Configuration.copyGlobal();

  @Rule
  protected TestLoggerRule mTestLogger = new TestLoggerRule();

  protected FileSystem mFileSystem;
  protected FileSystemContext mFileContext;
  protected ClientContext mClientContext;
  protected FileSystemMasterClient mFileSystemMasterClient;

  private class DummyAlluxioFileSystem extends BaseFileSystem {
    public DummyAlluxioFileSystem(FileSystemContext fsContext) {
      super(fsContext);
    }
  }

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    mConf.set(PropertyKey.USER_FILE_INCLUDE_OPERATION_ID, false);
    mClientContext = ClientContext.create(mConf);
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    when(mFileContext.acquireMasterClientResource()).thenReturn(
        new CloseableResource<FileSystemMasterClient>(mFileSystemMasterClient) {
          @Override
          public void closeResource() {
            // Noop.
          }
        });
    when(mFileContext.getClientContext()).thenReturn(mClientContext);
    when(mFileContext.getClusterConf()).thenReturn(mConf);
    when(mFileContext.getPathConf(any())).thenReturn(mConf);
    when(mFileContext.getUriValidationEnabled()).thenReturn(true);
    mFileSystem = new DummyAlluxioFileSystem(mFileContext);
  }

  @After
  public void after() {
    mConf = Configuration.copyGlobal();
  }

  /**
   * Verifies and releases the master client after a test with a filesystem operation.
   */
  public void verifyFilesystemContextAcquiredAndReleased() {
    verify(mFileContext).acquireMasterClientResource();
  }
}
