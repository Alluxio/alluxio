package alluxio.worker.ucx;

import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.ByteBufferInputStream;
import alluxio.util.io.ByteBufferOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class UcxMessage {
  private long mMessageId;
  private Type mRPCMessageType;

  public long getMessageId() {
    return mMessageId;
  }

  public Type getRPCMessageType() {
    return mRPCMessageType;
  }

  public ByteBuffer getRPCMessage() {
    return mRPCMessage;
  }

  private ByteBuffer mRPCMessage;

  public UcxMessage(long messageId,
                    Type RPCMessageType,
                    ByteBuffer RPCMessage) {
    mMessageId = messageId;
    mRPCMessageType = RPCMessageType;
    mRPCMessage = RPCMessage;
  }

  public static UcxMessage fromByteBuffer(ByteBuffer buffer) throws IOException {
    try (ByteBufferInputStream bbis = ByteBufferInputStream.getInputStream(buffer)) {
      long msgId = bbis.readLong();
      int msgType = bbis.readInt();
      Type rpcMessageType = Type.values()[msgType];
      int rpcMesgLength = bbis.readInt();
      // replace with getting one from memory pool
      ByteBuffer rpcMessage = ByteBuffer.allocateDirect(rpcMesgLength);
      bbis.read(rpcMessage, rpcMesgLength);
      rpcMessage.clear();
      UcxMessage ucxMessage = new UcxMessage(msgId, rpcMessageType, rpcMessage);
      return ucxMessage;
    }
  }

  public static void toByteBuffer(UcxMessage message, ByteBufferOutputStream bbos)
      throws IOException {
    bbos.writeLong(message.mMessageId);
    bbos.writeInt(message.mRPCMessageType.ordinal());
    int rpcMsgLen = message.getRPCMessage() != null ? message.getRPCMessage().remaining() : 0;
    bbos.writeInt(rpcMsgLen);
    if (rpcMsgLen > 0) {
      bbos.write(message.getRPCMessage(), rpcMsgLen);
    }
  }

  public enum Type {
    ReadRequest(() -> new ReadRequestStreamHandler(), Stage.IO),
    ReadRMARequest(() -> new ReadRequestRMAHandler(), Stage.IO),
    Reply(null, Stage.IO);
    ;

    public final Supplier<? extends UcxRequestHandler> mHandlerSupplier;
    public final Stage mStage;

    Type(Supplier<? extends UcxRequestHandler> handlerSupplier, Stage stage) {
      mHandlerSupplier = handlerSupplier;
      mStage = stage;
    }
  }

  public enum Stage {
    DEFAULT(new ThreadPoolExecutor(4, 4, 0,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(65536),
        ThreadFactoryUtils.build("DEFAULT-STAGE-%d", true))),
    IO(new ThreadPoolExecutor(4, 8, 0,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(65536),
        ThreadFactoryUtils.build("IO-STAGE-%d", true))),
    METADATA(new ThreadPoolExecutor(4, 8, 0,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(65536),
        ThreadFactoryUtils.build("METADATA-STAGE-%d", true)))
    ;

    public final ThreadPoolExecutor mThreadPool;

    Stage(ThreadPoolExecutor tpe) {
      mThreadPool = tpe;
    }
  }

}
