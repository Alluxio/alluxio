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

package alluxio.master;

import alluxio.Server;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;

import java.util.Collections;
import java.util.Map;

/**
 * This interface contains common operations for all masters.
 */
public interface Master extends Journaled, Server<Boolean> {
  /**
   * @return a journal context for writing journal entries to the master
   */
  JournalContext createJournalContext() throws UnavailableException;

  /**
   * @return a master context
   */
  MasterContext getMasterContext();

  /**
   * @return a map from service names to gRPC services that serve RPCs for this master,
   * if the master is a standby master.
   */
  default Map<ServiceType, GrpcService> getStandbyServices() {
    return Collections.emptyMap();
  }
}
