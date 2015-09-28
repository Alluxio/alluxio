/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.examples;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.util.io.PathUtils;

/**
 * Contains performance test cases where a bunch of Tachyon clients concurrently
 * operates on Tachyon master.
 */
public final class MasterConcurrencyPerformance {
  private static enum OperationType {
    CREATE_EMPTY_FILE,
  }

  private static interface OperationCallable {
    /**
     * What this operation will do on the path.
     *
     * @param path the path this operation operates on
     * @throws Exception when any unexpected exception happens
     */
    void call(TachyonURI path) throws Exception;
  }

  /** Registry where type of operation is mapped to concrete operation */
  private static Map<OperationType, OperationCallable> sOperationRegistry = Maps.newHashMap();

  static {
    final TachyonFileSystem tfs = TachyonFileSystem.get();

    /**
     * New operation types should first be added to {@link OperationType}, and then register the
     * {@link OperationCallable} for this type below in this static block, just use {@link #tfs}
     * when {@link TachyonFileSystem} is needed.
     */
    sOperationRegistry.put(OperationType.CREATE_EMPTY_FILE, new OperationCallable() {
      @Override
      public void call(TachyonURI path) throws Exception {
        tfs.create(path);
      }
    });
  }

  /**
   * A thread containing Tachyon client to do one kind of operation on a list of paths sequentially,
   * a bunch of such threads can run concurrently to simulate concurrent access to Tachyon master.
   */
  private static class ClientThread extends Thread {
    private static enum State {
      READY,
      RUNNING,
      SUCCEED,
      FAIL,
    }

    private OperationType mOpType;
    private OperationCallable mOp;
    /** List of paths to operate on */
    private List<TachyonURI> mPaths;
    /** When the thread starts */
    private long mStartTimeMs;
    /** When the thread stops, may be either the time of success or failure */
    private long mStopTimeMs;

    /**
     * State of this thread, will be initialized to {@link State#READY}, then it will be transferred
     * to {@link State#RUNNING} when the thread starts, if exception happens during the run, then
     * will be transferred to {@link State#FAIL}, otherwise, if the thread terminates properly,
     * then will be transferred to {@link State#SUCCEED}.
     *
     * This state will affect result returned by {@link #getReport()}.
     */
    private State mState;
    /** Exception thrown during running, will be shown in {@link #getReport()} */
    private Exception mFailureCause;

    /**
     * Instantiate a new client thread.
     *
     * @param opType the {@link OperationType} this thread does
     * @param paths list of paths where the operation will be done sequentially
     */
    public ClientThread(OperationType opType, List<TachyonURI> paths) {
      mOpType = opType;
      mOp = sOperationRegistry.get(opType);
      mPaths = paths;
      mState = State.READY;
    }

    @Override
    public void run() {
      mState = State.RUNNING;
      mStartTimeMs = System.currentTimeMillis();
      for (TachyonURI path : mPaths) {
        try {
          mOp.call(path);
        } catch (Exception e) {
          // No exception is expected, set state to FAIL and return.
          mStopTimeMs = System.currentTimeMillis();
          mState = State.FAIL;
          mFailureCause = e;
          return;
        }
      }
      mStopTimeMs = System.currentTimeMillis();
      mState = State.SUCCEED;

    }

    /**
     * @return life time of this thread no matter it succeeds or fails
     */
    public long getOperationTimeMs() {
      return mStopTimeMs - mStartTimeMs;
    }

    /**
     * @return a report for status of this thread, depends on {@link #mState}
     */
    public String getReport() {
      if (mState == State.READY) {
        return "Ready to be run but not running yet";
      } else if (mState == State.RUNNING) {
        return "Running";
      } else if (mState == State.FAIL) {
        return "client failed to do " + mOpType + " on all paths due to " + mFailureCause;
      } else {
        return "client did " + mOpType + " on " + mPaths.size() + " paths in "
            + getOperationTimeMs() + " ms";
      }
    }
  }

  /**
   * Create files by a bunch of clients concurrently with directory structure like
   * "/{client thread number}/{the number of files created by this client}".
   *
   * @param nFiles total number of files to create
   * @param nClients number of client threads
   * @throws InterruptedException
   */
  private static void createEmptyFilesUnderRoot(int nFiles, int nClients)
      throws InterruptedException {
    int nFilesEachThread = nFiles / nClients;
    List<ClientThread> clients = Lists.newArrayListWithExpectedSize(nClients);
    for (int client = 0; client < nClients; client ++) {
      int files = client == 0 ? nFiles - nFilesEachThread * (nClients - 1) : nFilesEachThread;
      List<TachyonURI> paths = Lists.newArrayListWithExpectedSize(files);
      for (int file = 0; file < files; file ++) {
        paths.add(new TachyonURI(PathUtils.concatPath("/", client, file)));
      }
      clients.add(new ClientThread(OperationType.CREATE_EMPTY_FILE, paths));
    }

    long startTimeMs = System.currentTimeMillis();
    for (ClientThread client : clients) {
      client.start();
    }
    for (ClientThread client : clients) {
      client.join();
      System.out.println(client.getReport());
    }
    long stopTimeMs = System.currentTimeMillis();

    System.out.println("Totally spent " + (stopTimeMs - startTimeMs) + " ms to create " + nFiles
        + " by " + nClients + " concurrently.");
  }

  public static void main(String[] args) throws Exception {
    // TODO(cc) Better command line interface.
    int nFiles = Integer.valueOf(args[0]);
    int nClients = Integer.valueOf(args[1]);
    createEmptyFilesUnderRoot(nFiles, nClients);
  }
}
