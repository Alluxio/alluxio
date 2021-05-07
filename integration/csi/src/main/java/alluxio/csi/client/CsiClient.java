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

package alluxio.csi.client;

import csi.v1.Csi;
import csi.v1.Csi.GetPluginInfoResponse;

import java.io.IOException;

/**
 * General interface for a CSI client. This interface defines all APIs
 * that CSI spec supports, including both identity/controller/node service
 * APIs.
 */
public interface CsiClient {

  /**
   * Gets some basic info about the CSI plugin, including the driver name,
   * version and optionally some manifest info.
   * @return {@link GetPluginInfoResponse}
   * @throws IOException when unable to get plugin info from the driver.
   */
  GetPluginInfoResponse getPluginInfo() throws IOException;

  /**
   * @param request the csi request
   * @return response
   */
  Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(
      Csi.ValidateVolumeCapabilitiesRequest request) throws IOException;

  /**
   * @param request the csi request
   * @return response
   */
  Csi.NodePublishVolumeResponse nodePublishVolume(
      Csi.NodePublishVolumeRequest request) throws IOException;

  /**
   * @param request the csi request
   * @return response
   */
  Csi.NodeUnpublishVolumeResponse nodeUnpublishVolume(
      Csi.NodeUnpublishVolumeRequest request) throws IOException;
}
