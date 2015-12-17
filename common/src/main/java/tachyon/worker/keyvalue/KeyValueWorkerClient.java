package tachyon.worker.keyvalue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.ClientBase;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.KeyValueWorkerService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.TachyonService;
import tachyon.thrift.TachyonTException;
import tachyon.util.network.NetworkAddressUtils;

/**
 * The client talks to a key-value worker server. It keeps sending keep alive message to the worker
 * server.
 *
 * Since {@link KeyValueWorkerService.Client} is not thread safe, this class has to guarantee thread
 * safety.
 */
public class KeyValueWorkerClient extends ClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private KeyValueWorkerService.Client mClient = null;

  /**
   * Creates a KeyValueWorkerClient.
   *
   * @param workerNetAddress location of the worker to connect to
   * @param conf Tachyon configuration
   */
  public KeyValueWorkerClient(NetAddress workerNetAddress, TachyonConf conf) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress), conf, "key-value-worker");
  }

  @Override
  protected TachyonService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.KEY_VALUE_WORKER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.KEY_VALUE_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new KeyValueWorkerService.Client(mProtocol);
  }

  /**
   * Notifies the worker to checkpoint the file asynchronously.
   *
   * @param blockId The id of the block
   * @return true if success, false otherwise
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized ByteBuffer get(final long blockId, final ByteBuffer key) throws IOException,
      TachyonException {
    return retryRPC(new ClientBase.RpcCallableThrowsTachyonTException<ByteBuffer>() {
      @Override
      public ByteBuffer call() throws TachyonTException, TException {
        return mClient.get(blockId, key);
      }
    });
  }
}
