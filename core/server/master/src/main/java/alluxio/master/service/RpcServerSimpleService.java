package alluxio.master.service;

import alluxio.master.MasterRegistry;

import java.net.InetSocketAddress;

/**
 * Manages the behavior of the master's simple service.
 */
public class RpcServerSimpleService implements SimpleService {
  private final MasterRegistry mMasterRegistry;
  private final InetSocketAddress mConnectAddress;
  private final InetSocketAddress mBindAddress;

  /**
   * Creates a simple service wrapper around a grpc server to manager the grpc server for the
   * master process.
   * @param masterRegistry where the grpc services will be drawn from
   * @param connectAddress the address where the rpc server will connect
   * @param bindAddress the address where the rpc server will bind
   */
  public RpcServerSimpleService(MasterRegistry masterRegistry, InetSocketAddress connectAddress,
      InetSocketAddress bindAddress) {
    mMasterRegistry = masterRegistry;
    mConnectAddress = connectAddress;
    mBindAddress = bindAddress;
  }

  @Override
  public void start() {

  }

  @Override
  public void promote() {

  }

  @Override
  public void demote() {

  }

  @Override
  public void stop() {

  }
}
