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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.file.ByteArrayTargetBuffer;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerNetAddress;

import io.grpc.Status;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public class NettyDataReaderStateMachineTest {

  // TODO(Yichuan):
  //  Considering final.
  //  Remove sleep.
  //  Replace all time with a parameter
  private final ExecutorService mExecutor = Executors.newSingleThreadExecutor();
  private static final EmbeddedChannel mChannel = new EmbeddedChannel();
  private static final String LOCAL_HOSTNAME = "localhost";
  private final ServerStateDriver
      mStateDriver = new ServerStateDriver(mExecutor, mChannel);
  private static FileSystemContext mFsContext;
  private static NettyDataReaderStateMachine nettyDataReaderStateMachine;
  private static final Protocol.ReadRequest.Builder
      mRequestBuilder = Protocol.ReadRequest.newBuilder();
  private static final WorkerNetAddress mWorkerNetAddress = new WorkerNetAddress()
      .setHost("127.0.0.1").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
  private static final int BYTEBUFFER_CAPACITY = 10;

  private Thread mMachineThread;

  private static byte[] mByteArray;

  @BeforeClass
  public static void beforeClass() throws IOException {
//    mFsContext = FileSystemContext.create();
    mFsContext = mock(FileSystemContext.class);
    when(mFsContext.acquireNettyChannel(any(WorkerNetAddress.class)))
        .thenReturn(mChannel);
    when(mFsContext.getClusterConf())
        .thenReturn(Configuration.global());
//    doNothing()
//        .when(mFsContext).releaseNettyChannel(any(WorkerNetAddress.class), any(Channel.class));
  }

  @Before
  public void before() {
    mByteArray = new byte[BYTEBUFFER_CAPACITY];
    nettyDataReaderStateMachine = new NettyDataReaderStateMachine(
        mFsContext, mWorkerNetAddress, mRequestBuilder,
        new ByteArrayTargetBuffer(mByteArray, 0));
    mMachineThread = new Thread(() -> {
      nettyDataReaderStateMachine.stepRun();
    });
  }

  // TODO(Yichuan): CHANNEL_UNAVAILABLE is not tested.

  @Test
  public void acquiringChannelToChannelActive() throws InterruptedException {
    // Have to sleep for a while to wait for the state machine thread.
    // spin loop? It is just another sleep wait.
    Thread.sleep(3000);
    assertEquals(NettyDataReaderStateMachine.State.CHANNEL_ACTIVE,
        nettyDataReaderStateMachine.getStatus());
  }

  @Test
  public void channelActiveTOReceivedData() throws InterruptedException {
    Thread.sleep(3000);
//    nettyDataReaderStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.INTERRUPTED);
    Thread.sleep(mFsContext.getClusterConf().getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS));
    assertEquals(NettyDataReaderStateMachine.State.CLIENT_CANCEL,
        nettyDataReaderStateMachine.getStatus());
  }

  @Test
  public void interruptTest() throws InterruptedException {

    mMachineThread.start();

    assertEquals(NettyDataReaderStateMachine.State.CREATED,
        nettyDataReaderStateMachine.getStatus());
    nettyDataReaderStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.START);
    nettyDataReaderStateMachine.stepRun();

    assertEquals(NettyDataReaderStateMachine.State.ACQUIRING_CHANNEL,
        nettyDataReaderStateMachine.getStatus());
    nettyDataReaderStateMachine.fireNext(
        NettyDataReaderStateMachine.TriggerEvent.CHANNEL_AVAILABLE);

    mMachineThread.start();
    mMachineThread.interrupt();
    Thread.sleep(3000);

//    Thread thread2 = new Thread(() -> nettyDataReaderStateMachine.stepRun());
//    thread2.start();
    nettyDataReaderStateMachine.stepRun();

    assertEquals(NettyDataReaderStateMachine.State.CLIENT_CANCEL,
        nettyDataReaderStateMachine.getStatus());
  }

  @Test
  public void timeOutTest() throws InterruptedException {

    mMachineThread.start();

    assertEquals(NettyDataReaderStateMachine.State.CREATED,
        nettyDataReaderStateMachine.getStatus());
    nettyDataReaderStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.START);
    nettyDataReaderStateMachine.stepRun();

    assertEquals(NettyDataReaderStateMachine.State.ACQUIRING_CHANNEL,
        nettyDataReaderStateMachine.getStatus());
    nettyDataReaderStateMachine.fireNext(
        NettyDataReaderStateMachine.TriggerEvent.CHANNEL_AVAILABLE);

    Thread thread1 = new Thread(() -> nettyDataReaderStateMachine.stepRun());

    thread1.start();
    Thread.sleep(30000 + 1000);

//    Thread thread2 = new Thread(() -> nettyDataReaderStateMachine.stepRun());
//    thread2.start();
    nettyDataReaderStateMachine.stepRun();

    assertEquals(NettyDataReaderStateMachine.State.CLIENT_CANCEL,
        nettyDataReaderStateMachine.getStatus());
  }

  @Test
  public void serverErrorTest() throws InterruptedException {
    // TODO(Yichuan): status cannot be set. Ask for handling it.
//    setToChannelActive();
    mMachineThread = new Thread(
        () -> {
          assertEquals(NettyDataReaderStateMachine.State.CREATED,
              nettyDataReaderStateMachine.getStatus());
          nettyDataReaderStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.START);
          nettyDataReaderStateMachine.stepRun();

          assertEquals(NettyDataReaderStateMachine.State.ACQUIRING_CHANNEL,
              nettyDataReaderStateMachine.getStatus());
          nettyDataReaderStateMachine.fireNext(
              NettyDataReaderStateMachine.TriggerEvent.CHANNEL_AVAILABLE);
          nettyDataReaderStateMachine.stepRun();
        }
    );
    mMachineThread.start();
    Thread.sleep(3000);
    // TODO(Yichuan): should not write like this. the server_error cannot interrupt the poll method,
    //  so the timeout event is triggered, instead server_error.
    nettyDataReaderStateMachine.fireNext(
        nettyDataReaderStateMachine.getTriggerEventsWithParam().mServerErrorEvent,
        new AlluxioStatusException(Status.CANCELLED));
    nettyDataReaderStateMachine.stepRun();
    assertEquals(NettyDataReaderStateMachine.State.CLIENT_CANCEL,
        nettyDataReaderStateMachine.getStatus());
  }

  @Test
  public void sendDataTest() throws ExecutionException, InterruptedException {

    final int length = 10;
    final int offset = 0;
    ServerState start = new WaitForRequestState(
        mRequestBuilder.clone().setLength(length).setOffset(offset).build());
    start.andThen(new SendDataState("hello".getBytes()))
        .andThen(new SendDataState("world".getBytes()))
        .andThen(new EofState());
    Future<Throwable> serverFault = mStateDriver.run(start);

    setStateToChannelActiveDirectly();

    mMachineThread.start();
    Thread.sleep(3000);
    assertEquals(NettyDataReaderStateMachine.State.CHANNEL_ACTIVE,
        nettyDataReaderStateMachine.getStatus());

    assertNull(serverFault.get());
  }

  private void setToChannelActive() {
    assertEquals(NettyDataReaderStateMachine.State.CREATED,
        nettyDataReaderStateMachine.getStatus());
    nettyDataReaderStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.START);
    nettyDataReaderStateMachine.stepRun();

    assertEquals(NettyDataReaderStateMachine.State.ACQUIRING_CHANNEL,
        nettyDataReaderStateMachine.getStatus());
    nettyDataReaderStateMachine.fireNext(
        NettyDataReaderStateMachine.TriggerEvent.CHANNEL_AVAILABLE);
  }

  private void setStateToChannelActiveDirectly() {
    nettyDataReaderStateMachine = new NettyDataReaderStateMachine(
        mFsContext,
        mWorkerNetAddress,
        mRequestBuilder,
        new ByteArrayTargetBuffer(mByteArray, 0),
        NettyDataReaderStateMachine.State.ACQUIRING_CHANNEL);
    nettyDataReaderStateMachine.setChannel(mChannel);
    nettyDataReaderStateMachine.fireNext(
        NettyDataReaderStateMachine.TriggerEvent.CHANNEL_AVAILABLE);
    nettyDataReaderStateMachine.stepRun();
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

  private static class WaitForRequestState extends ServerState {
    private final Protocol.ReadRequest mExpectedRequest;

    public WaitForRequestState(Protocol.ReadRequest readRequest) {
      this(Constants.SECOND_MS * 60, readRequest);
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

  private static class TerminalState extends ServerState {
    private static final TerminalState
        INSTANCE = new TerminalState();

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
    private BiConsumer<ServerStateDriver, ServerState>
        mAction = (cd, cs) -> { };
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
