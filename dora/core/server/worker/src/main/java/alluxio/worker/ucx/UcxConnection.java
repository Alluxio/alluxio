package alluxio.worker.ucx;

import alluxio.network.ucx.AlluxioUcxUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointErrorHandler;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucs.UcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handle all ucx connection related logics.
 * Connection establish / Disconnect / Error Handling etc.
 */
public class UcxConnection implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(UcxConnection.class);
  private long mTagToReceive = -1L;
  private long mTagToSend = -1L;
  private final UcpWorker mUcpWorker;
  private UcpEndpoint mEndpoint;
  private InetSocketAddress mRemoteAddress;
  private AtomicBoolean mClosed = new AtomicBoolean(false);
  // tag 0 is always reserved for general metadata exchange.
  private static final AtomicLong mTagGenerator = new AtomicLong(1L);
  // UcxConn to its own counter (for active msg or other usages... keep as a placeholder for now)
  private static final ConcurrentHashMap<UcxConnection, Set<ActiveRequest>>
      sRemoteConnections = new ConcurrentHashMap<>();
  // For streaming feature so we can send out-of-order data for multiplexing
  private AtomicLong mSequencer = new AtomicLong(1L);

  public UcxConnection(UcpWorker worker) {
    mUcpWorker = worker;
  }

  public long getTagToSend() {
    return mTagToSend;
  }

  public void setTagToSend(long tagToSend) {
    this.mTagToSend = tagToSend;
  }

  public long getTagToReceive() {
    return mTagToReceive;
  }

  public void setTagToReceive(long tagToReceive) {
    this.mTagToReceive = tagToReceive;
  }

  public UcpEndpoint getEndpoint() {
    return mEndpoint;
  }

  public void setEndpoint(UcpEndpoint mEndpoint) {
    this.mEndpoint = mEndpoint;
  }

  public InetSocketAddress getRemoteAddress() {
    return mRemoteAddress;
  }

  public void setRemoteAddress(InetSocketAddress mRemoteAddress) {
    this.mRemoteAddress = mRemoteAddress;
  }

  public long getNextSequence() {
    return mSequencer.incrementAndGet();
  }

  public UcpWorker getUcpWorker() {
    return mUcpWorker;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("UcpEndpoint", mEndpoint)
        .add("TagToSend", mTagToSend)
        .add("TagToReceive", mTagToReceive)
        .add("RemoteAddress", mRemoteAddress)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof UcxConnection)) {
      return false;
    }
    UcxConnection otherConn = (UcxConnection) other;
    return Objects.equal(otherConn.getTagToReceive(), getTagToReceive())
        && Objects.equal(otherConn.getTagToSend(), getTagToSend());
  }

  public void startRecvRPCRequest() {
    Preconditions.checkNotNull(mEndpoint, "UcpEndpoint is null, this should not happen.");
    // create a bytebuffer wrapped and protected by UcpMemory
    // TODO(lucy) pool this UcpMem and reuse for next recvRpc,
    // coz transfer into msg will have its own copy of buffer.
    UcpMemory recvMemoryBlock =
        UcxMemoryPool.allocateMemory(AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);

    ActiveRequest activeRequest = new ActiveRequest(this);
    activeRequest.setUcpMemory(recvMemoryBlock);
    final UcxConnection thisConn = this;
    UcpRequest recvRequest = mUcpWorker.recvTaggedNonBlocking(
        recvMemoryBlock.getAddress(), recvMemoryBlock.getLength(),
        mTagToReceive, 0xFFFFFFFFFFFFL, new UcxCallback() {
          public void onSuccess(UcpRequest request) {
            LOG.info("New req received from peer:{}", mEndpoint);
            try {
              // this entire memory block is owned and registered by ucx
              ByteBuffer rpcRecvBuffer = UcxUtils.getByteBufferView(
                  recvMemoryBlock.getAddress(), recvMemoryBlock.getLength());
              UcxMessage msg = UcxMessage.fromByteBuffer(rpcRecvBuffer);
//              sRemoteConnections.compute(thisConn, (conn, activeRequestSet) -> {
//                if (activeRequestSet == null) {
//                  return null;
//                }
//                try {
//                  activeRequest.close();
//                } catch (IOException e) {
//                  // actually there won't be checked exception thrown.
//                  LOG.error("Error of closing activeRequest:{}", activeRequest);
//                }
//                activeRequestSet.remove(activeRequest);
//                return activeRequestSet;
//              });
              UcxRequestHandler reqHandler = msg.getRPCMessageType().mHandlerSupplier.get();
              msg.getRPCMessageType().mStage.mThreadPool.execute(() -> {
                try {
                  reqHandler.handle(msg, thisConn);
                } catch (Throwable ex) {
                  LOG.error("Exception when handling req:{} from remote:{}",
                      msg, mEndpoint, ex);
                }
              });
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            LOG.info("onSuccess start receiving another req for remote:{}", mEndpoint);
            startRecvRPCRequest();
          }

          public void onError(int ucsStatus, String errorMsg) {
            LOG.error("Receive req errored, status:{}, errMsg:{}",
                ucsStatus, errorMsg);
            LOG.info("onError start receiving another req for remote:{}", mEndpoint);
            startRecvRPCRequest();
          }
        });
    activeRequest.setUcpRequest(recvRequest);
//    sRemoteConnections.compute(this, (conn, activeReqQ) -> {
//      if (activeReqQ == null) {
//        activeReqQ = new HashSet<>();
//      }
//      activeReqQ.add(activeRequest);
//      return activeReqQ;
//    });
  }

  public static class ActiveRequest implements Closeable {
    // pending ucprequest created on this particular UcxConnection
    private UcpRequest mUcpRequest;
    // The reference of pending memory allocated during creation of ucp requests
    // lifecycle of this memory block gets tracked and handled in here.
    private UcpMemory mMemoryBlock;
    private final UcxConnection mUcxConnection;

    public ActiveRequest(UcxConnection conn) {
      mUcxConnection = conn;
    }

    public void setUcpRequest(UcpRequest ucpRequest) {
      if (mUcpRequest == null) {
        mUcpRequest = ucpRequest;
      }
    }

    public void setUcpMemory(UcpMemory registerdMem) {
      if (mMemoryBlock == null) {
        mMemoryBlock = registerdMem;
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("UcpRequest status", mUcpRequest.getStatus())
          .add("UcpRequest recvSize", mUcpRequest.getRecvSize())
          .add("UcpRequest senderTag", mUcpRequest.getSenderTag())
          .add("UcpMemory length", mMemoryBlock.getLength())
          .toString();
    }

    @Override
    public void close() throws IOException {
      /* don't know how to close UcpRequest properly, it seems in recvTaggedNonBlockingNative
      jucx_request_allocate creates a new global ref of the newly created jucx_request(UcpRequest)
      to pass into ucp_request_param_t but never explicitly delete this global ref.
      Currently finding out reason or if there's a bug in OpenUcx community.
      */
      if (mUcpRequest != null) {
        mUcxConnection.getUcpWorker().cancelRequest(mUcpRequest);
      }
      if (mMemoryBlock != null) {
        mMemoryBlock.close();
      }
    }
  }


  public static UcxConnection initNewConnection(InetSocketAddress remoteAddr, UcpWorker worker)
      throws Exception {
    LOG.info("Initiating server connection for {}", remoteAddr);
    UcpEndpoint bootstrapEp = worker.newEndpoint(
        new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setErrorHandler((ep, status, errorMsg) ->
                LOG.error("[ERROR] creating ep to remote:"
                    + remoteAddr + " errored out: " + errorMsg
                    + " status:" + status + ",ep:" + ep.toString()))
            .setSocketAddress(remoteAddr));

    UcxConnection newConnection = new UcxConnection(worker);
    newConnection.setRemoteAddress(remoteAddr);
    newConnection.setTagToReceive(mTagGenerator.incrementAndGet());
    // generate tag to recv from remote, build up connectionEstablishBuf

    ByteBuffer establishConnBuf = ByteBuffer.allocateDirect(AlluxioUcxUtils.METADATA_SIZE_COMMON);
    AlluxioUcxUtils.writeConnectionMetadata(newConnection.getTagToSend(),
        newConnection.getTagToReceive(),establishConnBuf, worker);
    establishConnBuf.clear();
    // no need to register this buffer to UcpMemory,
    // will go out of scope once call back is done handling
    UcpRequest sendReq = bootstrapEp.sendStreamNonBlocking(
        UcxUtils.getAddress(establishConnBuf), establishConnBuf.capacity(), null);
    worker.progressRequest(sendReq);
    establishConnBuf.clear();
    UcpRequest recvReq = bootstrapEp.recvStreamNonBlocking(
        UcxUtils.getAddress(establishConnBuf), establishConnBuf.capacity(),
        UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null);
    worker.progressRequest(recvReq);
    // Parse coming-back buf
    // From the sender of the buf's point of view:
    // long(tag for send ) | long (tag for receive) | int (worker addr size) | bytes (worker addr)
    // check UcxUtils.buildConnectionMetadata for details
    establishConnBuf.clear();
    long tagRemoteToSend = establishConnBuf.getLong();
    long tagRemoteToReceive = establishConnBuf.getLong();
    int workerAddrSize = establishConnBuf.getInt();
    ByteBuffer workerAddr = ByteBuffer.allocateDirect(workerAddrSize);
    establishConnBuf.limit(establishConnBuf.position() + workerAddrSize);
    workerAddr.put(establishConnBuf);

    UcpEndpoint remoteEp = worker.newEndpoint(new UcpEndpointParams()
        .setErrorHandler(new UcxConnectionErrorHandler(newConnection))
        .setPeerErrorHandlingMode()
        .setUcpAddress(workerAddr));
    newConnection.setEndpoint(remoteEp);

    Preconditions.checkArgument(tagRemoteToSend == newConnection.getTagToReceive(),
        "Mismatch on the tag I assigned to remote");
    newConnection.setTagToSend(tagRemoteToReceive);

    bootstrapEp.closeNonBlockingFlush();
    return newConnection;
  }

  public static UcxConnection acceptIncomingConnection(
      UcpEndpoint bootstrapEp, UcpWorker worker, InetSocketAddress remoteAddr)
      throws Exception {
    UcxConnection newConnection = new UcxConnection(worker);
    newConnection.setRemoteAddress(remoteAddr);
    ByteBuffer establishConnBuf = ByteBuffer.allocateDirect(AlluxioUcxUtils.METADATA_SIZE_COMMON);
    UcpRequest recvReq = bootstrapEp.recvStreamNonBlocking(
        UcxUtils.getAddress(establishConnBuf), establishConnBuf.capacity(),
        UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null);
    newConnection.setEndpoint(bootstrapEp);
    worker.progressRequest(recvReq);
    establishConnBuf.clear();
    long tagRemoteToSend = establishConnBuf.getLong();
    long tagRemoteToReceive = establishConnBuf.getLong();
    int workerAddrSize = establishConnBuf.getInt();
    ByteBuffer workerAddr = ByteBuffer.allocateDirect(workerAddrSize);
    establishConnBuf.limit(establishConnBuf.position() + workerAddrSize);
    workerAddr.put(establishConnBuf);

    Preconditions.checkArgument(tagRemoteToSend == -1,
        "I haven't assigned tag for remote to receive");
    // generate tag for remote to receive
    tagRemoteToSend = mTagGenerator.incrementAndGet();
    newConnection.setTagToSend(tagRemoteToReceive);
    newConnection.setTagToReceive(tagRemoteToSend);

    UcpEndpoint remoteEp = worker.newEndpoint(new UcpEndpointParams()
        .setErrorHandler(new UcxConnectionErrorHandler(newConnection))
        .setPeerErrorHandlingMode()
        .setUcpAddress(workerAddr));
    newConnection.setEndpoint(remoteEp);

    // build establishConnBuf
    establishConnBuf.clear();
    AlluxioUcxUtils.writeConnectionMetadata(newConnection.getTagToSend(),
        newConnection.getTagToReceive(),establishConnBuf, worker);
    establishConnBuf.clear();
    UcpRequest sendReq = bootstrapEp.sendStreamNonBlocking(
        UcxUtils.getAddress(establishConnBuf), establishConnBuf.capacity(), new UcxCallback() {
          public void onError(int ucsStatus, String errorMsg) {
            LOG.error("error in acking to remote conn establishment req. Closing new UcxConnection..." +
                    "ucsStatus:{},errorMsg:{}"
              , ucsStatus, errorMsg);
            newConnection.close();
            throw new UcxException(errorMsg);
          }
        });
    worker.progressRequest(sendReq);
    // no need for this oob establish connection ep, close it
    bootstrapEp.closeNonBlockingFlush();
    return newConnection;
  }

  public boolean isClosed() {
    return mClosed.get();
  }

  public void close() {
    if (!mClosed.compareAndSet(false, true)) {
      LOG.warn("UcxConnection:{} already closed.", this);
    }
    if (mEndpoint != null) {
      LOG.warn("Closing remote ep:{}", mEndpoint);
      mEndpoint.closeNonBlockingFlush();
    }
  }

  static class UcxConnectionErrorHandler implements UcpEndpointErrorHandler {
    private final UcxConnection mUcxConnection;
    public UcxConnectionErrorHandler(UcxConnection ucxConnection) {
      mUcxConnection = ucxConnection;
    }

    @Override
    public void onError(UcpEndpoint errorHandlingEndpoint, int status, String errorMsg)
        throws Exception {
      LOG.warn("Error in connection:{}, closing related resources...", mUcxConnection);
      UcpEndpoint remoteEndpoint = mUcxConnection.getEndpoint();
      if (remoteEndpoint != null) {
        LOG.info("Closing remoteEp:{} on error, status:{}:errorMsg:{}", remoteEndpoint, status, errorMsg);
        remoteEndpoint.close();
      }
    }
  }
}
