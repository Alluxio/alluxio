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

package alluxio.master.file.mdsync;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.underfs.UfsStatus;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DummySyncProcess implements SyncProcess {

  @Override
  public SyncProcessResult performSync(
      LoadResult loadResult, UfsSyncPathCache syncPathCache) throws Throwable {

    Stream<UfsStatus> stream = loadResult.getUfsLoadResult().getItems().peek(status -> {
      // If we are loading by directory, then we must create a new load task on each
      // directory traversed
      if (loadResult.getTaskInfo().hasDirLoadTasks() && status.isDirectory()) {
        try {
          AlluxioURI fullPath = loadResult.getBaseLoadPath().join(status.getName());
          // first check if the directory needs to be synced
          if (syncPathCache.shouldSyncPath(
              fullPath, // no reverse resolve in test
              loadResult.getTaskInfo().getSyncInterval(),
              loadResult.getTaskInfo().getDescendantType()).isShouldSync()) {
            loadResult.getTaskInfo().getMdSync()
                .loadNestedDirectory(loadResult.getTaskInfo().getId(), fullPath);
          }
        } catch (InvalidPathException e) {
          throw new InvalidArgumentRuntimeException(e);
        }
      }
    });
    List<UfsStatus> items = stream.collect(Collectors.toList());
    if (items.size() == 0) {
      return new SyncProcessResult(loadResult.getTaskInfo(), loadResult.getBaseLoadPath(),
          null, false, false);
    }
    boolean rootPathIsFile = items.size() == 1 && loadResult.getBaseLoadPath().equals(
        loadResult.getTaskInfo().getBasePath()) && !items.get(0).isDirectory();
    return new SyncProcessResult(loadResult.getTaskInfo(), loadResult.getBaseLoadPath(),
        new PathSequence(new AlluxioURI(items.get(0).getName()),
            new AlluxioURI(items.get(items.size() - 1).getName())),
        loadResult.getUfsLoadResult().isTruncated(), rootPathIsFile
    );
  }
}

