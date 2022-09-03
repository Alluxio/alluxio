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

package alluxio.fsmaster;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.util.io.PathUtils;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileSystemBase {
  public enum ServerType { ALLUXIO_GRPC_SERVER, BASIC_GRPC_SERVER, STANDALONE }

  private ServerType mServerType;
  private Process mServerProcess;
  public ArrayList<ManagedChannel> mChannels = new ArrayList<>();

  public void init(ServerType serverType, int numGrpcChannels, String ipAddress, int port)
      throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);
    mServerType = serverType;
    if (serverType != ServerType.STANDALONE) {
      String java = PathUtils.concatPath(System.getProperty("java.home"), "bin", "java");
      String classpath = System.getProperty("java.class.path");
      List<String> args = new ArrayList<>(Arrays.asList(java, "-cp", classpath,
          BenchStandaloneGrpcServer.class.getCanonicalName(),
          "--server", serverType.name(),
          "--port", Integer.toString(port)));
      // communicate with the independent process to recover the port the server is bound to
      try (ServerSocket socket = new ServerSocket(0)) {
        args.addAll(Arrays.asList("--client-socket", Integer.toString(socket.getLocalPort())));
        ProcessBuilder pb = new ProcessBuilder(args);
        mServerProcess = pb.start();
        try (Socket accept = socket.accept();
             DataInputStream stream = new DataInputStream(accept.getInputStream())) {
          port = stream.readInt();
        }
      }
    }
    // disabling authentication as it does not pertain to the measurements in this benchmark
    // in addition, authentication would only happen once at the beginning and would be negligible
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL);
    Configuration.set(PropertyKey.MASTER_HOSTNAME, ipAddress);
    Configuration.set(PropertyKey.MASTER_RPC_PORT, port);
    for (int i = 0; i < numGrpcChannels; i++) {
      mChannels.add(ManagedChannelBuilder.forAddress(ipAddress, port).usePlaintext().build());
    }
  }

  public void tearDown() {
    Assert.assertNotNull(mServerType);
    mChannels.forEach(ManagedChannel::shutdown);
    if (mServerType != ServerType.STANDALONE) {
      mServerProcess.destroy();
    }
  }
}
