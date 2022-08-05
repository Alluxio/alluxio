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

package alluxio.master.file.meta.cross.cluster;

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks connections between clusters for cross cluster sync.
 */
public class CrossClusterConnections implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterConnections.class);

  private final Map<Set<InetSocketAddress>, FileSystemCrossCluster> mClients = new HashMap<>();

  void addStream(InvalidationStream stream) {
    InstancedConfiguration conf = Configuration.copyGlobal();
    List<InetSocketAddress> addressList = Arrays.asList(
        stream.getMountSyncAddress().getAddresses());
    FileSystemCrossCluster fs = mClients.computeIfAbsent(
        new HashSet<>(addressList),
        key -> alluxio.client.file.FileSystemCrossCluster.Factory.create(
            FileSystemContext.create(conf, addressList))
    );
    CompletableFuture.runAsync(() -> {
      try {
        fs.subscribeInvalidations(stream.getMountSyncAddress().getMountSync().getUfsPath(), stream);
      } catch (IOException | AlluxioException e) {
        stream.onError(e);
      }
    });
  }

  /**
   * @return the map of addresses to clients
   */
  @VisibleForTesting
  public Map<Set<InetSocketAddress>, FileSystemCrossCluster> getClients() {
    return mClients;
  }

  void removeClient(Set<InetSocketAddress> addresses) {
    FileSystemCrossCluster fs = mClients.remove(addresses);
    if (fs != null) {
      try {
        fs.close();
      } catch (IOException e) {
        LOG.warn("Error closing client", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    mClients.values().removeIf(fs -> {
      try {
        fs.close();
      } catch (IOException e) {
        LOG.warn("Error closing client", e);
      }
      return true;
    });
  }

  private static class AddressList {
    private final InetSocketAddress[] mAddresses;

    private AddressList(InetSocketAddress[] addresses) {
      mAddresses = addresses.clone();
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder(mAddresses.length);
      for (int i = 0; i < mAddresses.length; i++) {
        builder.append(mAddresses[i]);
        if (i != mAddresses.length - 1) {
          builder.append(";");
        }
      }
      return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AddressList that = (AddressList) o;
      return Arrays.equals(mAddresses, that.mAddresses);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(mAddresses);
    }
  }
}
