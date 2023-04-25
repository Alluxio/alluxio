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

package alluxio.master.file.scheduler;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.UnauthenticatedRuntimeException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.FileIterable;
import alluxio.master.job.LoadJob;

import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class FileIterableTest {

  @Test
  public void testException()
      throws FileDoesNotExistException, AccessControlException, IOException, InvalidPathException {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    String path = "test";
    doThrow(new FileDoesNotExistException(path)).when(fileSystemMaster).checkAccess(any(), any());
    FileIterable fileIterable = new FileIterable(fileSystemMaster, path, Optional.of("user"), false,
        LoadJob.QUALIFIED_FILE_FILTER);
    assertThrows(NotFoundRuntimeException.class, fileIterable::iterator);
    doThrow(new InvalidPathException(path)).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(NotFoundRuntimeException.class, fileIterable::iterator);
    doThrow(new AccessControlException(path)).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(UnauthenticatedRuntimeException.class, fileIterable::iterator);
  }
}
