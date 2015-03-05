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

package tachyon.perf;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.thrift.MasterService;
import tachyon.perf.thrift.SlaveAlreadyRegisterException;
import tachyon.perf.thrift.SlaveNotRegisterException;

/**
 * The client side of Tachyon-Perf Master.
 */
public class MasterClient implements Closeable {
  private static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);
  private static final int MAX_CONNECT_TRY = 5;

  private final String mMasterHostname;
  private final int mMasterPort;

  private MasterService.Client mClient = null;
  private TProtocol mProtocol = null;
  private volatile boolean mConnected;

  public MasterClient() {
    mMasterHostname = PerfConf.get().TACHYON_PERF_MASTER_HOSTNAME;
    mMasterPort = PerfConf.get().TACHYON_PERF_MASTER_PORT;
    mConnected = false;

    mProtocol =
        new TBinaryProtocol(new TFramedTransport(new TSocket(mMasterHostname, mMasterPort)));
    mClient = new MasterService.Client(mProtocol);
  }

  /**
   * Clean the connect.
   */
  @Override
  public synchronized void close() throws IOException {
    if (mConnected) {
      LOG.info("Disconnecting from Tachyon-Perf Master " + mMasterHostname + ":" + mMasterPort);
      mConnected = false;
    }
    if (mProtocol != null) {
      mProtocol.getTransport().close();
    }
  }

  /**
   * Try to connect to the Tachyon-Perf Master.
   * 
   * @return true if success, false otherwise
   */
  private synchronized boolean connect() {
    if (!mConnected) {
      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        return false;
      }
      mConnected = true;
      LOG.info("Connect to Tachyon-Perf Master " + mMasterHostname + ":" + mMasterPort);
    }
    return mConnected;
  }

  /**
   * Connects to the Tachyon-Perf Master. An exception is thrown if this fails.
   * 
   * @throws IOException
   */
  public synchronized void mustConnect() throws IOException {
    int tries = 0;
    while (tries ++ < MAX_CONNECT_TRY) {
      if (connect()) {
        return;
      }
    }
    throw new IOException("Failed to connect to the Tachyon-Perf Master");
  }

  /**
   * Check if all the slaves are ready so this slave can start to run.
   * 
   * @param taskId the id of this slave
   * @param nodeName the name of this slave
   * @return true if all the slaves are ready to run
   * @throws IOException
   */
  public synchronized boolean slave_canRun(int taskId, String nodeName) throws IOException {
    mustConnect();
    try {
      return mClient.slave_canRun(taskId, nodeName);
    } catch (SlaveNotRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify Tachyon-Perf Master that this slave is finished.
   * 
   * @param taskId the id of this slave
   * @param nodeName the name of this slave
   * @param successFinish true if this slave finished successfully, false otherwise
   * @throws IOException
   */
  public synchronized void slave_finish(int taskId, String nodeName, boolean successFinish)
      throws IOException {
    mustConnect();
    try {
      mClient.slave_finish(taskId, nodeName, successFinish);
    } catch (SlaveNotRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify Tachyon-Perf Master that this slave is ready to run.
   * 
   * @param taskId the id of this slave
   * @param nodeName the name of this slave
   * @param successSetup true if this slave setup successfully, false otherwise
   * @throws IOException
   */
  public synchronized void slave_ready(int taskId, String nodeName, boolean successSetup)
      throws IOException {
    mustConnect();
    try {
      mClient.slave_ready(taskId, nodeName, successSetup);
    } catch (SlaveNotRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Register this slave to the Tachyon-Perf Master.
   * 
   * @param taskId the id of this slave
   * @param nodeName the name of this slave
   * @param cleanupDir if not null, it will cleanup this directory after all the slaves are finished
   * @return true if register successfully, false otherwise
   * @throws IOException
   */
  public synchronized boolean slave_register(int taskId, String nodeName, String cleanupDir)
      throws IOException {
    if (!connect()) {
      return false;
    }
    try {
      return mClient.slave_register(taskId, nodeName, cleanupDir);
    } catch (SlaveAlreadyRegisterException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }
}
