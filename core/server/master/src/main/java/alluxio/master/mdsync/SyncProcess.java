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

package alluxio.master.mdsync;

import alluxio.AlluxioURI;
import alluxio.underfs.UfsStatus;

import java.util.List;
import java.util.stream.Collectors;

class SyncProcess {

  static SyncProcessResult performSync(LoadResult loadResult) throws Throwable {
    List<UfsStatus> items = loadResult.getUfsLoadResult().getItems().collect(Collectors.toList());
    boolean rootPathIsFile = items.size() == 1 && loadResult.getBaseLoadPath().equals(
        loadResult.getTaskInfo().getBasePath()) && !items.get(0).isDirectory();
    return new SyncProcessResult(loadResult.getTaskInfo(), loadResult.getBaseLoadPath(),
        new PathSequence(new AlluxioURI(items.get(0).getName()),
            new AlluxioURI(items.get(items.size() - 1).getName())),
        loadResult.getUfsLoadResult().isTruncated(), rootPathIsFile);
  }
}
