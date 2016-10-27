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

import alluxio.Server;

import org.apache.thrift.TProcessor;

import java.util.Map;

/**
 * Interface of an Alluxio worker.
 */
public interface Worker extends Server {
  /**
   * @return a map from service names to {@link TProcessor}s that serve RPCs for this worker
   */
  Map<String, TProcessor> getServices();
}
