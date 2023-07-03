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

package alluxio.worker;

import alluxio.Process;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.modules.AlluxioWorkerProcessModule;
import alluxio.worker.modules.DoraWorkerModule;
import alluxio.worker.modules.GrpcServerModule;
import alluxio.worker.modules.NettyServerModule;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.net.InetSocketAddress;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A worker in the Alluxio system.
 */
public interface WorkerProcess extends Process {
  /**
   * Factory for creating {@link WorkerProcess}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link WorkerProcess}
     */
    public static WorkerProcess create() {
      // read configurations
      boolean isNettyDataTransmissionEnable =
          Configuration.global().getBoolean(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED);
      // add modules that need to be injected
      ImmutableList.Builder<Module> modules = ImmutableList.builder();
      modules.add(new DoraWorkerModule());
      modules.add(new GrpcServerModule());
      modules.add(new NettyServerModule(isNettyDataTransmissionEnable));
      modules.add(new AlluxioWorkerProcessModule());

      // inject the modules
      Injector injector = Guice.createInjector(modules.build());
      return injector.getInstance(WorkerProcess.class);
    }

    private Factory() {
    } // prevent instantiation
  }

  /**
   * @return the connect information for this worker
   */
  WorkerNetAddress getAddress();

  /**
   * @return the block worker for this Alluxio worker
   */
  UfsManager getUfsManager();

  /**
   * @return the worker's data service bind host (used by unit test only)
   */
  String getDataBindHost();

  /**
   * @return the worker's data service port (used by unit test only)
   */
  int getDataLocalPort();

  /**
   * @return the worker's netty data service port (used by unit test only)
   */
  int getNettyDataLocalPort();

  /**
   * @return the worker's data service domain socket path if available or "" if not available
   */
  String getDataDomainSocketPath();

  /**
   * @return this worker's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the worker in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the worker in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the worker web service bind host (used by unit test only)
   */
  String getWebBindHost();

  /**
   * @return the worker web service port (used by unit test only)
   */
  int getWebLocalPort();

  /**
   * @param clazz the class of the worker to get
   * @param <T>   the type of the worker to get
   * @return the given worker
   */
  <T extends Worker> T getWorker(Class<T> clazz);
}
