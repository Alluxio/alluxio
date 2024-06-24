package alluxio.worker.ucx;

import alluxio.PositionReader;
import alluxio.file.ReadTargetBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.network.ucx.AlluxioUcxUtils;
import alluxio.util.io.ByteBufferOutputStream;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucs.UcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public class UcxDataReader implements PositionReader {
  private static final Logger LOG = LoggerFactory.getLogger(UcxDataReader.class);

  InetSocketAddress mAddr;
  private static InetSocketAddress sLocalAddr = null;

  UcxConnection mConnection;
  private ReentrantLock acquireConnLock = new ReentrantLock();
  // make this a global, one per process only instance
  UcpWorker mWorker;

  Supplier<Protocol.ReadRequest.Builder> mRequestBuilder;
  Supplier<Protocol.ReadRequestRMA.Builder> mRequestRMABuilder;
  public UcxDataReader(InetSocketAddress addr, UcpWorker worker,
                       @Nullable Protocol.ReadRequest.Builder requestBuilder,
                       @Nullable Protocol.ReadRequestRMA.Builder requestRMABuilder) {
    try {
      sLocalAddr = new InetSocketAddress(InetAddress.getLocalHost(),0);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    mAddr = addr;
    mWorker = worker;
    mRequestBuilder = requestBuilder != null ? requestBuilder::clone : null;
    mRequestRMABuilder = requestRMABuilder != null ? requestRMABuilder::clone : null;
  }

  public void acquireServerConn() throws IOException {
    if (mConnection == null || mConnection.isClosed()) {
      try {
        acquireConnLock.lock();
        LOG.debug("Connecting to : {}", mAddr);
        if (mConnection == null || mConnection.isClosed()) {
          mConnection = UcxConnection.initNewConnection(mAddr, mWorker);
        }
      } catch (Exception e) {
        throw new IOException(
            String.format("Error initializing conn with remote:%s", mAddr), e);
      } finally {
        acquireConnLock.unlock();
      }
    }
  }

  synchronized public int progressWorker() throws Exception {
    return mWorker.progress();
  }

  public void waitForRequest(UcpRequest ucpRequest) {
    while(!ucpRequest.isCompleted()) {
      try {
        progressWorker();
      } catch (Exception e) {
        LOG.error("Error progressing req:", e);
      }
    }
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length) throws IOException {
    // use Stream API
    if (mRequestBuilder != null) {
      return readInternalStream(position, buffer, length);
    }
    // use RMA API
    if (mRequestRMABuilder != null) {
      Preconditions.checkArgument(buffer.byteBuffer().isDirect(), "ByteBuffer must be direct buffer");
      Preconditions.checkArgument(buffer.byteBuffer().position() == 0,
          "Provided ByteBuffer holder has to be empty");
      // register this result memory buffer
      UcpMemory resultMemBlock = UcxMemoryPool.registerMemory(
          UcxUtils.getAddress(buffer.byteBuffer()), length);
      return readInternalRMA(position, resultMemBlock, length);
    }
    return -1;
  }

  public int readInternalRMA(long position, UcpMemory resultMemoryBlock, int length)
      throws IOException {
    Preconditions.checkNotNull(mRequestRMABuilder, "Must provide ReadRequestRMA");
    // pack this memory region info and send read req over.
    // pack to rkey buf
    ByteBuffer rkeyBuf = resultMemoryBlock.getRemoteKeyBuffer();
    Protocol.ReadRequestRMA.Builder builder = mRequestRMABuilder.get()
        .setLength(length)
        .setOffset(position)
        .setRemoteMemAddr(resultMemoryBlock.getAddress())
        .setRkeyBuf(ByteString.copyFrom(rkeyBuf))
        .clearCancel();
    Protocol.ReadRequestRMA readRequest = builder.build();
    UcxMessage readRMAMessage = new UcxMessage(0, UcxMessage.Type.ReadRMARequest,
        ByteBuffer.wrap(readRequest.toByteArray()));

    UcpMemory ucxMesgMem = UcxMemoryPool.allocateMemory(
        AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
    ByteBuffer ucxMesgMemBuffer = UcxUtils.getByteBufferView(ucxMesgMem.getAddress(), ucxMesgMem.getLength());
    try (ByteBufferOutputStream bbos = ByteBufferOutputStream.getOutputStream(ucxMesgMemBuffer)) {
      UcxMessage.toByteBuffer(readRMAMessage, bbos);
    }
    ucxMesgMemBuffer.clear();
    UcpRequest sendRequest = mConnection.getEndpoint().sendTaggedNonBlocking(
        ucxMesgMemBuffer, mConnection.getTagToSend(), new UcxCallback() {
          public void onSuccess(UcpRequest request) {
            LOG.debug("ReadRMARequest:{} sent.", readRequest);
            ucxMesgMem.deregister();
          }

          public void onError(int ucsStatus, String errorMsg) {
            ucxMesgMem.deregister();
            throw new UcxException(errorMsg);
          }
        });
    LOG.debug("Waiting for read request to send...");
    waitForRequest(sendRequest);

    // read with RMA
    UcpMemory relyMemoryBlock = UcxMemoryPool.allocateMemory(
        AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
    UcpRequest replyReq = mConnection.getEndpoint().recvStreamNonBlocking(relyMemoryBlock.getAddress(),
        relyMemoryBlock.getLength(), 0, null);
    LOG.debug("Waiting for RMA done signal reception...");
    waitForRequest(replyReq);
    UcxMessage replyMessage = UcxMessage.fromByteBuffer(UcxUtils.getByteBufferView(
        relyMemoryBlock.getAddress(), relyMemoryBlock.getLength()));
    Protocol.ReadResponseRMA rmaReadResponse = Protocol.ReadResponseRMA.parseFrom(
        replyMessage.getRPCMessage());
    relyMemoryBlock.deregister();
    resultMemoryBlock.deregister(); // now target buffer available for caller to access
    LOG.debug("rmaReadResponse:{}", rmaReadResponse);
    return (int)rmaReadResponse.getReadLength();
  }

  public int readInternalStream(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    Protocol.ReadRequest.Builder builder = mRequestBuilder.get()
        .setLength(length)
        .setOffset(position)
        .clearCancel();
    Protocol.ReadRequest readRequest = builder.build();
    UcxMessage readMessage = new UcxMessage(0, UcxMessage.Type.ReadRequest,
        ByteBuffer.wrap(readRequest.toByteArray()));
    UcpMemory ucxMesgMem = UcxMemoryPool.allocateMemory(
        AlluxioUcxUtils.METADATA_SIZE_COMMON,
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
    ByteBuffer ucxMesgMemBuffer = UcxUtils.getByteBufferView(ucxMesgMem.getAddress(), ucxMesgMem.getLength());
    try (ByteBufferOutputStream bbos = ByteBufferOutputStream.getOutputStream(ucxMesgMemBuffer)) {
      UcxMessage.toByteBuffer(readMessage, bbos);
    }
    ucxMesgMemBuffer.clear();
    UcpRequest sendRequest = mConnection.getEndpoint().sendTaggedNonBlocking(
        ucxMesgMemBuffer, mConnection.getTagToSend(), new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        LOG.debug("ReadReq:{} sent.", readRequest);
        ucxMesgMem.deregister();
      }

      public void onError(int ucsStatus, String errorMsg) {
        ucxMesgMem.deregister();
        throw new UcxException(errorMsg);
      }
    });
    LOG.debug("Waiting for read request to send...");
    waitForRequest(sendRequest);
    // now wait to recv data
    Preconditions.checkArgument(buffer.byteBuffer().isDirect(), "ByteBuffer must be direct buffer");
    int bytesRead = 0;
    ByteBuffer preamble = ByteBuffer.allocateDirect(16);
    TreeMap<Long, ByteBuffer> buffers = new TreeMap<>();
    preamble.clear();
    LinkedList<UcpRequest> dataUcpRecvReqs = new LinkedList<>();
    while (bytesRead < length) {
      UcpRequest recvReq = mConnection.getEndpoint().recvStreamNonBlocking(UcxUtils.getAddress(preamble), 16,
          UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new UcxCallback() {
            public void onSuccess(UcpRequest request) {}

            public void onError(int ucsStatus, String errorMsg) {
              throw new UcxException(errorMsg);
            }
          });
      LOG.debug("Waiting for preamble...");
      waitForRequest(recvReq);
      preamble.clear();
      long seq = preamble.getLong();
      long size = preamble.getLong();
      preamble.clear();
      ByteBuffer seqBuffer = ByteBuffer.allocateDirect(8);
      ByteBuffer dataBuffer = ByteBuffer.allocateDirect((int)size);
      long[] addrs = new long[2];
      long[] sizes = new long[2];
      addrs[0] = UcxUtils.getAddress(seqBuffer);
      addrs[1] = UcxUtils.getAddress(dataBuffer);
      sizes[0] = 8;
      sizes[1] = size;
      LOG.debug("preamble info:seq:{}:len:{}", seq, size);
      UcpRequest dataRecvReq = mConnection.getEndpoint().recvStreamNonBlocking(addrs, sizes,
          UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new UcxCallback() {
            public void onSuccess(UcpRequest request) {
              ByteBuffer seqBufView = UcxUtils.getByteBufferView(addrs[0], sizes[0]);
              seqBufView.clear();
              long sequence = seqBufView.getLong();
              ByteBuffer dataBufView = UcxUtils.getByteBufferView(addrs[1], sizes[1]);
              dataBufView.clear();
              LOG.debug("Received buffers, seq:{}, data buf size:{}", sequence, sizes[1]);
              buffers.put(sequence, dataBufView);
            }

            public void onError(int ucsStatus, String errorMsg) {
              LOG.error("Error receiving buffers, seq:{}, data buf size:{}, errorMsg:{}",
                  seq, size, errorMsg);
              throw new UcxException(errorMsg);
            }
          });
      LOG.debug("Offering actual data recReq to q...");
      dataUcpRecvReqs.offer(dataRecvReq);
//      waitForRequest(recvReq);
      bytesRead += size;
    }
    while(!dataUcpRecvReqs.isEmpty()) {
      UcpRequest nextReq = dataUcpRecvReqs.poll();
      waitForRequest(nextReq);
    }
    buffer.byteBuffer().clear();
    while (!buffers.isEmpty()) {
      Map.Entry<Long, ByteBuffer> entry = buffers.pollFirstEntry();
      LOG.debug("Copying seq:{},bufsize:{}", entry.getKey(), entry.getValue());
      entry.getValue().clear();
      buffer.byteBuffer().put(entry.getValue());
    }
    return 0;
  }

  @Override
  public void close() {
    if (mConnection != null) {
      mConnection.close();
    }
  }
}
