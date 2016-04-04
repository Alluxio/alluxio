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

package alluxio.perf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;

import alluxio.perf.conf.PerfConf;
import alluxio.perf.thrift.MasterService;

/**
 * Entry point for the Alluxio-Perf Master program.
 */
public class AlluxioPerfMaster {
  class MasterServiceThread extends Thread {
    TNonblockingServerSocket mServerTNonblockingServerSocket = null;
    TServer mMasterServiceServer = null;

    public void setup(String hostname, int port) throws Exception {
      InetSocketAddress address = new InetSocketAddress(hostname, port);
      mServerTNonblockingServerSocket = new TNonblockingServerSocket(address);
      MasterServiceHandler masterServiceHandler = new MasterServiceHandler(mSlaveStatus);
      MasterService.Processor<MasterServiceHandler> masterServiceProcessor =
          new MasterService.Processor<MasterServiceHandler>(masterServiceHandler);
      mMasterServiceServer =
          new TThreadedSelectorServer(new TThreadedSelectorServer.Args(
              mServerTNonblockingServerSocket).processor(masterServiceProcessor).selectorThreads(3)
              .acceptQueueSizePerThread(3000)
              .workerThreads(Runtime.getRuntime().availableProcessors()));
    }

    @Override
    public void run() {
      mMasterServiceServer.serve();
    }

    public void shutdown() {
      this.interrupt();
      if (mMasterServiceServer != null) {
        mMasterServiceServer.stop();
      }
      if (mServerTNonblockingServerSocket != null) {
        mServerTNonblockingServerSocket.close();
      }
    }
  }

  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  private static void abortAllSlaves() {
    try {
      java.lang.Runtime.getRuntime().exec(
          PerfConf.get().ALLUXIO_PERF_HOME + "/bin/alluxio-perf-abort");
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error(e);
    }
  }

  public static void main(String[] args) {
    int slavesNum = 0;
    Set<String> slaves = null;
    String testCase = null;
    try {
      int index = 0;
      slavesNum = Integer.parseInt(args[0]);
      slaves = new HashSet<String>(slavesNum);
      for (index = 1; index < slavesNum + 1; index ++) {
        String slaveName = (index - 1) + "@" + args[index];
        if (!slaves.add(slaveName)) {
          throw new Exception("Slave name replicated: " + slaveName);
        }
      }
      testCase = args[index];
      System.out.println("Supervise Alluxio-Perf " + testCase + " Test");
    } catch (Exception e) {
      abortAllSlaves();
      e.printStackTrace();
      System.err.println("Wrong arguments. Should be <SlavesNum> [Slaves...] <TestCase>");
      LOG.error("Wrong arguments. Should be <SlavesNum> [Slaves...] <TestCase>", e);
    }
    AlluxioPerfMaster master = new AlluxioPerfMaster(slavesNum, slaves);
    if (!master.start()) {
      abortAllSlaves();
      System.err.println("Error when start Alluxio-Perf Master");
      LOG.error("Error when start Alluxio-Perf Master");
    }
    try {
      master.stop();
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error when stop Alluxio-Perf Master");
      LOG.error("Error when stop Alluxio-Perf Master", e);
    }
  }

  private MasterServiceThread mMasterServiceThread;
  private SlaveStatus mSlaveStatus;

  public AlluxioPerfMaster(int slavesNum, Set<String> slaves) {
    mSlaveStatus = new SlaveStatus(slavesNum, slaves);
    mMasterServiceThread = new MasterServiceThread();
  }

  public boolean start() {
    if (!startMasterService()) {
      return false;
    }
    System.out.println("Wait all slaves register...");
    LOG.info("Wait all slaves register...");
    if (!waitRegister()) {
      return false;
    }
    System.out.println("Wait all slaves finished...");
    LOG.info("Wait all slaves finished...");
    return waitFinish();
  }

  private boolean startMasterService() {
    try {
      mMasterServiceThread.setup(PerfConf.get().ALLUXIO_PERF_MASTER_HOSTNAME,
          PerfConf.get().ALLUXIO_PERF_MASTER_PORT);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error(e);
      return false;
    }
    mMasterServiceThread.start();
    return true;
  }

  public void stop() throws Exception {
    mSlaveStatus.cleanup();
    stopMasterService();
  }

  private void stopMasterService() throws Exception {
    mMasterServiceThread.shutdown();
  }

  private boolean waitFinish() {
    PerfConf conf = PerfConf.get();
    while (true) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return false;
      }
      String info = mSlaveStatus.getFinishStatus(conf.STATUS_DEBUG);
      System.out.println(info);
      LOG.info(info);
      int state = mSlaveStatus.finished(conf.FAILED_THEN_ABORT, conf.FAILED_PERCENTAGE);
      if (state == -1) {
        System.err.println("Enough slaves failed. Abort all the slaves.");
        LOG.error("Enough slaves failed. Abort all the slaves.");
        return true;
      }
      if (state == 1) {
        return true;
      }
    }
  }

  private boolean waitRegister() {
    long limitMs = System.currentTimeMillis() + PerfConf.get().UNREGISTER_TIMEOUT_MS;
    while (System.currentTimeMillis() < limitMs) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        LOG.error(e);
        return false;
      }
      if (mSlaveStatus.allRegistered()) {
        return true;
      }
    }
    Set<String> remains = mSlaveStatus.getUnregisterSlaves();
    StringBuffer sbInfo = new StringBuffer("Unregister time out:");
    for (String slave : remains) {
      sbInfo.append(" ").append(slave);
    }
    System.err.println(sbInfo.toString());
    LOG.error(sbInfo.toString());
    return false;
  }
}
