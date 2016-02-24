/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.mesos;

import org.apache.mesos.Protos;

import alluxio.Constants;
import alluxio.Configuration;

/**
 * Mesos framework offer utils
 */
public final class OfferUtils {
  private static final Configuration sConf = new Configuration();

  private OfferUtils() {

  }

  /**
   * @param offer Resource offer from Mesos
   * @return Return true if the master port available in this offer
   */
  public static boolean hasAvailableMasterPorts(Protos.Offer offer) {
    Protos.Value.Ranges ranges = getOfferedPorts(offer);

    return ranges != null
        && hasAvailablePorts(sConf.getInt(Constants.MASTER_WEB_PORT), ranges)
        && hasAvailablePorts(sConf.getInt(Constants.MASTER_RPC_PORT), ranges);
  }

  /**
   * @param offer Resource offer from Mesos
   * @return Return true if the worker port available in this offer
   */
  public static boolean hasAvailableWorkerPorts(Protos.Offer offer) {
    Protos.Value.Ranges ranges = getOfferedPorts(offer);

    return ranges != null
        && hasAvailablePorts(sConf.getInt(Constants.WORKER_WEB_PORT), ranges)
        && hasAvailablePorts(sConf.getInt(Constants.WORKER_RPC_PORT), ranges)
        && hasAvailablePorts(sConf.getInt(Constants.WORKER_DATA_PORT), ranges);
  }

  private static boolean hasAvailablePorts(int port, Protos.Value.Ranges ranges) {
    for (Protos.Value.Range range : ranges.getRangeList()) {
      if (port >= range.getBegin() && port <= range.getEnd()) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param offer Resource offer from Mesos
   * @return Ports ranges
   */
  public static Protos.Value.Ranges getOfferedPorts(Protos.Offer offer) {
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (Constants.MESOS_RESOURCE_PORTS.equals(resource.getName())) {
        return resource.getRanges();
      }
    }
    return null;
  }

  /**
   * @param offer Resource offer from Mesos
   * @return offered cpus size
   */
  public static double getOfferedCpus(Protos.Offer offer) {
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (Constants.MESOS_RESOURCE_CPUS.equals(resource.getName())) {
        return resource.getScalar().getValue();
      }
    }
    return 0.0d;
  }

  /**
   * @param offer Resource offer from Mesos
   * @return offered memory size
   */
  public static double getOfferedMem(Protos.Offer offer) {
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (Constants.MESOS_RESOURCE_MEM.equals(resource.getName())) {
        return resource.getScalar().getValue();
      }
    }
    return 0.0d;
  }

  /**
   * @param offer Resource offer from Mesos
   * @return offered disk size
   */
  public static double getOfferedDisk(Protos.Offer offer) {
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (Constants.MESOS_RESOURCE_DISK.equals(resource.getName())) {
        return resource.getScalar().getValue();
      }
    }
    return 0.0d;
  }
}
