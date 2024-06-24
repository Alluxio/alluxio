package alluxio.worker.ucx;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UcpServer {
  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);

  public static final UcpContext sGlobalContext = new UcpContext(new UcpParams()
      .requestStreamFeature()
      .requestTagFeature()
      .requestRmaFeature()
      .requestWakeupFeature());
  public static final int BIND_PORT = Configuration.getInt(PropertyKey.WORKER_UCPSERVER_PORT);

  private UcpWorker mGlobalWorker;
  public CacheManager mCacheManager;
  // TODO(lucy) backlogging if too many incoming req...
  private LinkedBlockingQueue<UcpConnectionRequest> mConnectionRequests
      = new LinkedBlockingQueue<>();
  private ExecutorService mAcceptorExecutor =  Executors.newFixedThreadPool(1);

  /*
   TODO for testing purpose without dependency on worker to provide
   cache manager capability, ufs path is local file path for now
   */
  public void prefill(String ufsPath, int fileSize) {
    final long pageSize = Configuration.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    int offset = 0;
    while (offset < fileSize) {
      long pageIdx = offset / pageSize;
      PageId pageId = new PageId(new AlluxioURI(ufsPath).hash(), pageIdx);
      int bytesToCache = Math.min((int)pageSize, fileSize - offset);
      final int pos = offset;
      Supplier<byte[]> externalDataSupplier = () -> {
        byte[] bytes = new byte[bytesToCache];
        try (RandomAccessFile file = new RandomAccessFile(
            new AlluxioURI(ufsPath).getPath(), "r")) {
          file.seek(pos);
          file.read(bytes);
          return bytes;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
      int cached = mCacheManager.cache(pageId, CacheContext.defaults(), externalDataSupplier);
      offset += cached;
    }
  }

  @Inject
  public UcpServer(
      CacheManager cacheManager) throws IOException {
    Preconditions.checkNotNull(cacheManager, "Need CacheManager instance.");
    mCacheManager = cacheManager;
    mGlobalWorker = sGlobalContext.newWorker(new UcpWorkerParams()
        .requestWakeupRMA()
        .requestThreadSafety());
  }

  public void start() {
    LOG.info("Starting UcpServer...");
    // TODO(lucy) now its binding to all addrs, could choose NIC with "ib-"
    // to select out those IB cards addrs to bind.
    List<InetAddress> addressesToBind = getAllAddresses();
    UcpListenerParams listenerParams = new UcpListenerParams()
        .setConnectionHandler(connectionRequest -> {
          LOG.debug("Incoming request, clientAddr:{} clientId:{}",
              connectionRequest.getClientAddress(), connectionRequest.getClientId());
          mConnectionRequests.offer(connectionRequest);
        });
    for (InetAddress addr : addressesToBind) {
      UcpListener ucpListener = mGlobalWorker.newListener(listenerParams.setSockAddr(
          new InetSocketAddress(addr, BIND_PORT)));
      LOG.info("Bound UcpListener on address:{}", ucpListener.getAddress());
    }
    mAcceptorExecutor.submit(new AcceptorThread());
    LOG.info("UcpServer started.");
  }

  public void awaitTermination() {
    mAcceptorExecutor.shutdown();
    try {
      mAcceptorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<InetAddress> getAllAddresses() {
    // Get all NIC addrs
    Stream<NetworkInterface> nics = Stream.empty();
    try {
      nics = Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
          .filter(iface -> {
            try {
              return iface.isUp() && !iface.isLoopback() &&
                  !iface.isVirtual() &&
                  !iface.getName().contains("docker");
              // identify infiniband usually interface name looks like ib-...
            } catch (SocketException e) {
              return false;
            }
          });
    } catch (SocketException e) {
    }
    List<InetAddress> addresses = nics.flatMap(iface ->
            Collections.list(iface.getInetAddresses()).stream())
        .filter(addr -> !addr.isLinkLocalAddress())
        .collect(Collectors.toList());
    return addresses;
  }

  // accept one single rpc req at a time
  class AcceptorThread implements Runnable {

    public void acceptNewConn() {
      UcpConnectionRequest connectionReq = mConnectionRequests.poll();
      if (connectionReq != null) {
        try {
          UcpEndpoint bootstrapEp = mGlobalWorker.newEndpoint(new UcpEndpointParams()
              .setPeerErrorHandlingMode()
              .setConnectionRequest(connectionReq));
          UcxConnection ucxConnection = UcxConnection.acceptIncomingConnection(
              bootstrapEp, mGlobalWorker, connectionReq.getClientAddress());
          ucxConnection.startRecvRPCRequest();
        } catch (Exception e) {
          LOG.error("Error in acceptNewConn:", e);
        }
      }
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          acceptNewConn();
          while (mGlobalWorker.progress() == 0) {
            LOG.debug("nothing to progress. wait for events..");
            try {
              mGlobalWorker.waitForEvents();
            } catch (Exception e) {
              LOG.error(e.getLocalizedMessage());
            }
          }
        } catch (Exception e) {
          // not sure what exception would be thrown here.
          LOG.error("Exception in AcceptorThread:", e);
        }
      }
    }
  }

}