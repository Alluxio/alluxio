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

package alluxio.cross.cluster.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.client.file.ListStatusPartialResult;
import alluxio.client.file.URIStatus;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utils for the cross cluster latency bench.
 */
public class CrossClusterLatencyUtils {

  /**
   * Wait for the clusters to be synced on a path.
   * @param path the path to check
   * @param clients the clients, one per cluster
   */
  public static void waitConsistent(AlluxioURI path, Iterable<FileSystemCrossCluster> clients)
      throws Exception {
    CommonUtils.waitFor("Clusters not fully synced", () -> {
      List<URIStatus> result = checkClusterConsistency(path, StreamSupport.stream(
          clients.spliterator(), false));
      if (result != null) {
        System.err.printf("Clusters not yet synced, fond different files at same index %s",
            result.stream().map(nxt -> Optional.ofNullable(nxt).map(URIStatus::getPath))
                .collect(Collectors.toList()));
        return false;
      }
      return true;
    }, WaitForOptions.defaults().setTimeoutMs(30_000));
  }

  /**
   * @return null if all clients have the same files in the path, or the list of files
   * at the first index where a difference was found
   * @param path the path to check
   * @param clients lists of clients, one for each cluster
   */
  public static List<URIStatus> checkClusterConsistency(
      AlluxioURI path, Stream<FileSystemCrossCluster> clients) {
    List<Iterator<URIStatus>> iterators = clients.map((client) ->
        listIteratorForClient(path, client)).collect(Collectors.toList());

    while (true) {
      List<URIStatus> nextCheck = iterators.stream().map((nxtIter) -> {
        if (!nxtIter.hasNext()) {
          return null;
        }
        return nxtIter.next();
      }).collect(Collectors.toList());

      // all streams are done if all returned null
      if (nextCheck.stream().noneMatch(Objects::nonNull)) {
        return null;
      }
      for (URIStatus nxt : nextCheck) {
        if (!nxt.getPath().equals(nextCheck.get(0).getPath())) {
          // a different was found
          return nextCheck;
        }
      }
    }
  }

  /**
   * @param path the path to iterate
   * @param client the client
   * @return an iterator for the given path and client
   */
  public static Iterator<URIStatus> listIteratorForClient(AlluxioURI path, FileSystem client) {
    ListStatusPartialPOptions.Builder options = ListStatusPartialPOptions.newBuilder()
        .setBatchSize(1000).setOptions(ListStatusPOptions.newBuilder().setRecursive(true).build());

    return new Iterator<URIStatus>() {
      Iterator<URIStatus> mRemain = Collections.emptyIterator();
      String mStartAfter = "";

      @Override
      public boolean hasNext() {
        if (!mRemain.hasNext()) {
          try {
            ListStatusPartialResult result = client.listStatusPartial(path,
                options.setStartAfter(mStartAfter).build());
            mRemain = result.getListings().iterator();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        return mRemain.hasNext();
      }

      @Override
      public URIStatus next() {
        URIStatus next = mRemain.next();
        mStartAfter = next.getPath();
        return next;
      }
    };
  }

  private CrossClusterLatencyUtils() {}
}
