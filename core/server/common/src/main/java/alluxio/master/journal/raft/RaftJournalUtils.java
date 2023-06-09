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

package alluxio.master.journal.raft;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Helper class for raft journal operations.
 */
public class RaftJournalUtils {
  public static final String RAFT_DIR = "raft";

  private RaftJournalUtils() {
    // prevent instantiation
  }

  /**
   * Gets the raft peer id.
   *
   * @param address the address of the server
   * @return the raft peer id
   */
  public static RaftPeerId getPeerId(InetSocketAddress address) {
    return getPeerId(address.getHostString(), address.getPort());
  }

  /**
   * Gets the raft peer id.
   *
   * @param host the hostname of the server
   * @param port the port of the server
   * @return the raft peer id
   */
  public static RaftPeerId getPeerId(String host, int port) {
    return RaftPeerId.getRaftPeerId(host + "_" + port);
  }

  /**
   * Gets the raft journal dir.
   *
   * @param baseDir the journal base dir
   * @return the raft peer id
   */
  public static File getRaftJournalDir(File baseDir) {
    return new File(baseDir, RAFT_DIR);
  }

  /**
   * Creates a temporary snapshot file.
   *
   * @param storage the snapshot storage
   * @return the temporary snapshot file
   * @throws IOException if error occurred while creating the snapshot file
   */
  public static File createTempSnapshotFile(SimpleStateMachineStorage storage) throws IOException {
    File tempDir = new File(storage.getSmDir().getParentFile(), "tmp");
    if (!tempDir.isDirectory() && !tempDir.mkdir()) {
      throw new IOException(
          "Cannot create temporary snapshot directory at " + tempDir.getAbsolutePath());
    }
    return File.createTempFile("raft_snapshot_" + System.currentTimeMillis() + "_",
        ".dat", tempDir);
  }

  /**
   * Creates a future that is completed exceptionally.
   *
   * @param e the exception to be returned by the future
   * @param <T> the type of the future
   * @return the completed future
   */
  public static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }
}
