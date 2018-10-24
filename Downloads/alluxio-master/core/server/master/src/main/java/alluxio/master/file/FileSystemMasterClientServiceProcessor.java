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

package alluxio.master.file;

import alluxio.network.thrift.BootstrapServerTransport;
import alluxio.thrift.FileSystemMasterClientService;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alluxio customized Thrift processor to handle RPC and track client IP.
 *
 * Original Thrift framework does not expose client IP because:
 * a) there is no such interface to query IP.
 * b) IP is not stored by Thrift.
 * c) The last stop from Thrift framework into Alluxio land is the
 *    {@link org.apache.thrift.ProcessFunction#process} method of
 *    {@link org.apache.thrift.ProcessFunction} class. This method has the information of in
 *    protocol from which we can derive IP. However, this method is made final deliberately.
 *    Consequently, it is very hard to subclass {@link org.apache.thrift.ProcessFunction}
 *    and override process method to pass IP informaton to Alluxio via arguments.
 * Based on a), b) and c), we decide to subclass the auto-generated
 * {@link FileSystemMasterClientService.Processor} class and
 * override its {@link org.apache.thrift.ProcessFunction#process} method. In this method, we store
 * the client IP as a thread-local variable for future use by {@link DefaultFileSystemMaster}.
 */
public class FileSystemMasterClientServiceProcessor
    extends FileSystemMasterClientService.Processor {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterClientServiceProcessor.class);
  private static ThreadLocal<String> sClientIpThreadLocal = new ThreadLocal<>();

  /**
   * Constructs a {@link FileSystemMasterClientServiceProcessor} instance.
   *
   * @param handler user-specified handler to process RPC
   */
  public FileSystemMasterClientServiceProcessor(FileSystemMasterClientService.Iface handler) {
    super(handler);
  }

  /**
   * Retrieves the client IP of current thread.
   *
   * @return the IP of the client of current thread
   */
  public static String getClientIp() {
    return sClientIpThreadLocal.get();
  }

  @Override
  public boolean process(TProtocol in, TProtocol out) throws TException {
    TTransport transport = in.getTransport();
    if (transport instanceof BootstrapServerTransport) {
      transport = ((BootstrapServerTransport) transport).getBaseTransport();
    }
    if (transport instanceof TSocket) {
      String ip = ((TSocket) transport).getSocket().getInetAddress().toString();
      sClientIpThreadLocal.set(ip);
    } else {
      LOG.warn("Failed to obtain client IP: underlying transport is not TSocket but {}", transport);
      sClientIpThreadLocal.set(null);
    }
    return super.process(in, out);
  }
}
