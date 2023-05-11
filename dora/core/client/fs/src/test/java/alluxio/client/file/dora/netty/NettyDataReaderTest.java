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

package alluxio.client.file.dora.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.exception.status.UnknownException;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public class NettyDataReaderTest {
  private final ExecutorService mExecutor = Executors.newSingleThreadExecutor();
  private final EmbeddedChannel mChannel = new EmbeddedChannel();
  private final ServerStateDriver mStateDriver = new ServerStateDriver(mExecutor, mChannel);
  private final WorkerNetAddress mWorkerAddress = new WorkerNetAddress();
  private final Protocol.ReadRequest.Builder mRequestBuilder = Protocol.ReadRequest.newBuilder();
  private FileSystemContext mFsContext;

  private NettyDataReader mReader;

  @Before
  public void setup() throws Exception {
    mFsContext = mock(FileSystemContext.class);
    when(mFsContext.acquireNettyChannel(any(WorkerNetAddress.class)))
        .thenReturn(mChannel);
    when(mFsContext.getClusterConf())
        .thenReturn(Configuration.global());
    doNothing()
        .when(mFsContext).releaseNettyChannel(any(WorkerNetAddress.class), any(Channel.class));
    mReader = new NettyDataReader(mFsContext, mWorkerAddress, mRequestBuilder);
  }

  @After
  public void verifyChannelReleased() {
    verify(mFsContext).releaseNettyChannel(mWorkerAddress, mChannel);
  }

  @Test
  public void read() throws Exception {
    final int length = 10;
    final long offset = 0;
    byte[] byteArray = new byte[length];
    ServerState start = new WaitForRequestState(
        mRequestBuilder.clone().setLength(length).setOffset(offset).build());
    start.andThen(new SendDataState("hello".getBytes()))
        .andThen(new SendDataState("world".getBytes()))
        .andThen(new EofState());
    Future<Throwable> serverFault = mStateDriver.run(start);
    int bytesRead = mReader.read(offset, byteArray, length);

    assertNull(serverFault.get());
    assertEquals(length, bytesRead);
    checkResult("helloworld".getBytes(), byteArray);
  }

  @Test
  public void eof() throws Exception {
    final long offset = 0;
    final int length = 11;
    byte[] byteArray = new byte[length];
    ServerState start = new WaitForRequestState(
        mRequestBuilder.clone().setLength(length).setOffset(offset).build());
    start.andThen(new SendDataState("hello".getBytes()))
        .andThen(new SendDataState("world".getBytes()))
        .andThen(new EofState());
    Future<Throwable> serverFault = mStateDriver.run(start);
    int bytesRead = mReader.read(offset, byteArray, length);

    assertNull(serverFault.get());
    assertEquals(10, bytesRead);
    checkResult("helloworld".getBytes(), byteArray);
  }

  @Test
  public void serverError() throws Exception {
    final long offset = 0;
    final int length = 11;
    byte[] byteArray = new byte[length];
    final String serverErrorMsg = "server sent an exception";

    Protocol.ReadRequest.Builder builder = mRequestBuilder.clone()
        .setLength(length)
        .setOffset(offset);
    ServerState start = new WaitForRequestState(builder.clone().build());
    start.andThen(new SendDataState("hello".getBytes()))
        .andThen(new ErrorState(new UnknownException(serverErrorMsg)))
        .andThen(new WaitForRequestState(
            builder.clone().setCancel(true).build()))
        .andThen(new CancelState());
    Future<Throwable> serverFault = mStateDriver.run(start);
    PartialReadException exception = assertThrows(PartialReadException.class,
        () -> mReader.read(offset, byteArray, length));

    assertNull(serverFault.get());
    assertEquals(5, exception.getBytesRead());
    checkResult("hello".getBytes(), byteArray);
    Throwable serverException = exception.getCause();
    assertTrue(serverException instanceof UnknownException);
    assertTrue(serverException.getMessage().contains(serverErrorMsg));
  }

  @Test
  public void serverDisconnect() throws Exception {
    final long offset = 0;
    final int length = 11;
    byte[] byteArray = new byte[length];
    ServerState start = new WaitForRequestState(
        mRequestBuilder.clone().setLength(length).setOffset(offset).build());
    start.andThen(new SendDataState("hello".getBytes()))
        .andThen(new DisconnectState());
    Future<Throwable> serverFault = mStateDriver.run(start);
    PartialReadException exception = assertThrows(PartialReadException.class,
        () -> mReader.read(offset, byteArray, length));

    assertNull(serverFault.get());
    assertEquals(5, exception.getBytesRead());
    checkResult("hello".getBytes(), byteArray);
    Throwable cause = exception.getCause();
    assertTrue(cause instanceof UnavailableException);
  }

  @Test
  public void serverTimeout() throws Exception {
    final long offset = 0;
    final int length = 11;
    byte[] byteArray = new byte[length];
    InstancedConfiguration conf = Configuration.copyGlobal();
    conf.set(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS, 100);
    when(mFsContext.getClusterConf())
        .thenReturn(conf);
    Protocol.ReadRequest.Builder builder = mRequestBuilder.clone()
        .setLength(length)
        .setOffset(offset);
    mReader = new NettyDataReader(mFsContext, mWorkerAddress, builder);
    ServerState start = new WaitForRequestState(builder.clone().build());
    start.andThen(new DelayState(150))
        .andThen(new SendDataState("helloworld".getBytes()))
        .andThen(new EofState())
        .andThen(new WaitForRequestState(
            builder.clone().setCancel(true).build()))
        .andThen(new CancelState());
    Future<Throwable> serverFault = mStateDriver.run(start);

    PartialReadException exception = assertThrows(PartialReadException.class,
        () -> mReader.read(offset, byteArray, length));
    assertNull(serverFault.get());
    assertEquals(0, exception.getBytesRead());
    assertTrue(exception.getCause() instanceof TimeoutException);
  }

  @Test
  public void ufsHeartbeatResetsClientTimeout() throws Exception {
    final long offset = 0;
    final int length = 11;
    byte[] byteArray = new byte[length];

    InstancedConfiguration conf = Configuration.copyGlobal();
    conf.set(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS, 1000);
    when(mFsContext.getClusterConf())
        .thenReturn(conf);
    mReader = new NettyDataReader(mFsContext, mWorkerAddress, mRequestBuilder);
    ServerState start = new WaitForRequestState(
        mRequestBuilder.clone().setLength(length).setOffset(offset).build());
    // this sequence takes 1200 ms in total to complete, longer than client timeout
    // but heartbeat will reset the client timeout clock, so that read will succeed
    start.andThen(new DelayState(600))
        .andThen(new SendUfsHeartBeatState())
        .andThen(new SendDataState("hello".getBytes()))
        .andThen(new DelayState(600))
        .andThen(new SendUfsHeartBeatState())
        .andThen(new SendDataState("world".getBytes()))
        .andThen(new EofState());
    Future<Throwable> serverFault = mStateDriver.run(start);

    int bytesRead = mReader.read(offset, byteArray, length);
    assertNull(serverFault.get());
    assertEquals(10, bytesRead);
    checkResult("helloworld".getBytes(), byteArray);
  }

  private void checkResult(byte[] expected, byte[] actual) {
    assertTrue(expected.length <= actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }

  private static class ServerStateDriver {
    private final ExecutorService mExecutor;
    private final EmbeddedChannel mChannel;
    private ServerState mCurrentState;

    ServerStateDriver(ExecutorService executor, EmbeddedChannel channel) {
      mExecutor = executor;
      mChannel = channel;
    }

    public EmbeddedChannel getChannel() {
      return mChannel;
    }

    public Future<Throwable> run(ServerState initialState) {
      mCurrentState = initialState;
      return mExecutor.submit(() -> {
        if (mCurrentState.isTerminal()) {
          return null;
        }
        while (true) {
          try {
            mCurrentState.run(this);
          } catch (Throwable t) {
            return t;
          }
          if (mCurrentState.isTerminal()) {
            if (mChannel.outboundMessages().size() > 0) {
              Object message = mChannel.outboundMessages().remove();
              return new RuntimeException("Unhandled client message: " + message);
            }
            return null;
          }
          mCurrentState = mCurrentState.nextState();
        }
      });
    }
  }

  private static class DelayState extends ServerState {
    private final int mDelayMs;

    public DelayState(int delayMs) {
      mDelayMs = delayMs;
      action((cd, cs) -> {
        try {
          Thread.sleep(mDelayMs);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private static class WaitForRequestState extends ServerState {
    private final Protocol.ReadRequest mExpectedRequest;

    public WaitForRequestState(Protocol.ReadRequest readRequest) {
      this(Constants.SECOND_MS * 10, readRequest);
    }

    public WaitForRequestState(int timeout, Protocol.ReadRequest readRequest) {
      mExpectedRequest = readRequest;
      action((cd, cs) -> {
        try {
          CommonUtils.waitFor("read request", () -> {
            RPCProtoMessage message = cd.getChannel().readOutbound();
            return message != null
                && message.getType() == RPCMessage.Type.RPC_READ_REQUEST
                && mExpectedRequest.equals(message.getMessage().asReadRequest());
          }, WaitForOptions.defaults().setTimeoutMs(timeout));
        } catch (TimeoutException timeoutException) {
          throw new RuntimeException("waiting for read request timed out",
              timeoutException);
        } catch (InterruptedException interruptedException) {
          throw new RuntimeException(interruptedException);
        }
      });
    }
  }

  private static class DisconnectState extends ServerState {
    public DisconnectState() {
      action((cd, cs) -> {
        cd.getChannel().disconnect();
      });
    }
  }

  private static class ErrorState extends ServerState {
    public ErrorState(AlluxioStatusException exception) {
      action((cd, cs) -> {
        RPCProtoMessage response = RPCProtoMessage.createResponse(exception);
        cd.getChannel().writeInbound(response);
      });
    }
  }

  private static class CancelState extends ServerState {
    public CancelState() {
      action((cd, cs) -> {
        RPCProtoMessage response = RPCProtoMessage.createCancelResponse();
        cd.getChannel().writeInbound(response);
      });
    }
  }

  private static class EofState extends ServerState {
    public EofState() {
      action((cd, cs) -> {
        RPCProtoMessage response = RPCProtoMessage.createOkResponse(null);
        cd.getChannel().writeInbound(response);
      });
    }
  }

  private static class SendDataState extends ServerState {
    private final byte[] mBatch;

    public SendDataState(byte[] data) {
      mBatch = data;
      action((cd, cs) -> {
        RPCProtoMessage response = RPCProtoMessage.createOkResponse(
            new NettyDataBuffer(Unpooled.wrappedBuffer(mBatch)));
        cd.getChannel().writeInbound(response);
      });
    }
  }

  private static class SendUfsHeartBeatState extends ServerState {
    public SendUfsHeartBeatState() {
      action((cd, cs) -> {
        RPCProtoMessage response = new RPCProtoMessage(new ProtoMessage(
            Protocol.ReadResponse.newBuilder()
                .setType(Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT)
                .build()));
        cd.getChannel().writeInbound(response);
      });
    }
  }

  private static class TerminalState extends ServerState {
    private static final TerminalState INSTANCE = new TerminalState();

    @Override
    public boolean isTerminal() {
      return true;
    }

    @Override
    public ServerState nextState() {
      return INSTANCE;
    }
  }

  private static class ServerState {
    private BiConsumer<ServerStateDriver, ServerState> mAction = (cd, cs) -> { };
    private ServerState mNextState = TerminalState.INSTANCE;
    private boolean mIsTerminal = false;

    ServerState() { }

    ServerState action(BiConsumer<ServerStateDriver, ServerState> action) {
      mAction = action;
      return this;
    }

    void run(ServerStateDriver driver) {
      mAction.accept(driver, this);
    }

    ServerState nextState() {
      return mNextState;
    }

    ServerState andThen(ServerState next) {
      mNextState = next;
      return next;
    }

    ServerState terminal() {
      mIsTerminal = true;
      return this;
    }

    boolean isTerminal() {
      return mIsTerminal;
    }
  }
}
