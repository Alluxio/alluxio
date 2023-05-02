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

package alluxio.worker.modules;

import alluxio.worker.AlluxioWorkerProcess;
import alluxio.worker.WorkerProcess;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * Grpc worker module.
 */
public class AlluxioWorkerProcessModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(WorkerProcess.class).to(AlluxioWorkerProcess.class).in(Scopes.SINGLETON);
  }
}
