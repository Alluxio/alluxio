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

import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.csi.utils.GrpcHelper;

import csi.v1.Csi;
import csi.v1.Csi.GetPluginInfoRequest;
import csi.v1.Csi.GetPluginInfoResponse;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.SocketAddress;

/**
 * A CSI client implementation that communicates with a CSI driver via
 * unix domain socket. It leverages gRPC blocking stubs to synchronize
 * the call with CSI driver. CSI spec is designed as a set of synchronized
 * APIs, in order to make the call idempotent for failure recovery,
 * so the client does the same.
 */
public class CsiClientImpl implements CsiClient {

  private static final Option CSI_OPERATION_OPTION = Option.builder("op")
      .hasArg()
      .required(true)
      .longOpt("operation")
      .desc("nodePublishVolume,nodeUnpublishVolume,getPluginInfo")
      .build();
  private static final Option CSI_TARGET_OPTION = Option.builder("t")
      .hasArg()
      .required(false)
      .longOpt("target")
      .desc("target path")
      .build();
  private static final Option CSI_VOLUME_ID_OPTION = Option.builder("vid")
      .hasArg()
      .required(false)
      .longOpt("volumeId")
      .desc("volume id")
      .build();
  private static final Options OPTIONS = new Options()
      .addOption(CSI_OPERATION_OPTION)
      .addOption(CSI_TARGET_OPTION)
      .addOption(CSI_VOLUME_ID_OPTION);

  private final SocketAddress mAddress;

  /**
   * @param address socket address
   */
  public CsiClientImpl(String address) {
    mAddress = GrpcHelper.getSocketAddress(address);
  }

  @Override
  public GetPluginInfoResponse getPluginInfo() throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(mAddress).build()) {
      GetPluginInfoRequest request = GetPluginInfoRequest.getDefaultInstance();
      return client.createIdentityBlockingStub().getPluginInfo(request);
    }
  }

  @Override
  public Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(
      Csi.ValidateVolumeCapabilitiesRequest request) throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(mAddress).build()) {
      return client.createControllerBlockingStub()
          .validateVolumeCapabilities(request);
    }
  }

  @Override
  public Csi.NodePublishVolumeResponse nodePublishVolume(
      Csi.NodePublishVolumeRequest request) throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(mAddress).build()) {
      return client.createNodeBlockingStub()
          .nodePublishVolume(request);
    }
  }

  @Override
  public Csi.NodeUnpublishVolumeResponse nodeUnpublishVolume(
      Csi.NodeUnpublishVolumeRequest request) throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(mAddress).build()) {
      return client.createNodeBlockingStub()
          .nodeUnpublishVolume(request);
    }
  }

  @Nullable
  private static CsiClientOptions parseOptions(String[] args, AlluxioConfiguration alluxioConf) {
    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(OPTIONS, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(CsiClientImpl.class.getName(), OPTIONS);
        return null;
      }

      String operation = cli.getOptionValue(CSI_OPERATION_OPTION.getLongOpt());
      String target = cli.getOptionValue(CSI_TARGET_OPTION.getLongOpt());
      String volumeId = cli.getOptionValue(CSI_VOLUME_ID_OPTION.getLongOpt());

      return new CsiClientOptions(operation, target, volumeId);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(CsiClientImpl.class.getName(), OPTIONS);
      return null;
    }
  }

  /**
   * @param args arguments to run the command line
   */
  public static void main(String[] args) throws IOException {
    System.out.println(String.format("Alluxio version: {}-{}",
        RuntimeConstants.VERSION, ProjectConstants.REVISION));
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    CsiClientOptions opts = parseOptions(args, conf);
    if (opts == null) {
      System.exit(1);
    }
    CsiClientImpl client = new CsiClientImpl(conf.get(PropertyKey.CSI_DOMAIN_SOCKET_ADDRESS));
    switch (opts.getOperation()) {
      case "nodePublishVolume":
        Csi.NodePublishVolumeRequest request =
            Csi.NodePublishVolumeRequest.newBuilder()
                .setTargetPath(opts.getTarget())
                .setVolumeId(opts.getVolumeId())
                .build();
        client.nodePublishVolume(request);
        break;
      case "nodeUnpublishVolume":
        Csi.NodeUnpublishVolumeRequest request2 =
            Csi.NodeUnpublishVolumeRequest.newBuilder()
                .setTargetPath(opts.getTarget())
                .build();
        client.nodeUnpublishVolume(request2);
        break;
      case "getPluginInfo":
        GetPluginInfoResponse response = client.getPluginInfo();
        System.out.println("name = " + response.getName() + ", vendorVersion = "
            + response.getVendorVersion());
        break;
      default:
        break;
    }
  }
}
