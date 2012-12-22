package tachyon;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataServerMessage {
  public static final short DATA_SERVER_REQUEST_MESSAGE = 1;
  public static final short DATA_SERVER_RESPONSE_MESSAGE = 2;

  private final Logger LOG = LoggerFactory.getLogger(DataServerMessage.class); 

  private final boolean IS_TO_SEND_DATA;
  private final short mMsgType;
  private boolean mIsMessageReady;

  private ByteBuffer mHeader;
  private static final int HEADER_LENGTH = 12;
  private int mDatasetId;
  private int mPartitionId;
  private int mDataLength;
  RandomAccessFile mFile;

  private ByteBuffer mData;

  FileChannel mInChannel;

  private DataServerMessage(boolean isToSendData, short msgType) {
    IS_TO_SEND_DATA = isToSendData;
    mMsgType = msgType;
    mIsMessageReady = false;
  }

  public static DataServerMessage createPartitionRequestMessage() {
    DataServerMessage ret = new DataServerMessage(false, DATA_SERVER_REQUEST_MESSAGE);

    ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);

    return ret;
  }

  public static DataServerMessage createPartitionRequestMessage(int datasetId, int partitionId) {
    DataServerMessage ret = new DataServerMessage(true, DATA_SERVER_REQUEST_MESSAGE);

    ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
    ret.mDatasetId = datasetId;
    ret.mPartitionId = partitionId;
    ret.mDataLength = 0;
    ret.generateHeader();
    ret.mData = ByteBuffer.allocate(0);

    ret.mIsMessageReady = true;

    return ret;
  }

  public static DataServerMessage createPartitionResponseMessage(boolean toSend, 
      int datasetId, int partitionId) {
    DataServerMessage ret = new DataServerMessage(toSend, DATA_SERVER_RESPONSE_MESSAGE);

    if (toSend) {
      ret.mDatasetId = datasetId;
      ret.mPartitionId = partitionId;

      try {
        String filePath = Config.WORKER_DATA_FOLDER + datasetId + "-" + partitionId;
        ret.LOG.info("Try to response remote requst by reading from " + filePath); 
        ret.mFile = new RandomAccessFile(filePath, "r");
        ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
        ret.mDataLength = (int) ret.mFile.length();
        ret.mInChannel = ret.mFile.getChannel();
        ret.mData = ret.mInChannel.map(FileChannel.MapMode.READ_ONLY, 0, ret.mDataLength);
        ret.mIsMessageReady = true;
        ret.generateHeader();
        WorkerServiceHandler.sDataAccessQueue.add(CommonUtils.generateBigId(datasetId, partitionId));
      } catch (IOException e) {
        // TODO This is a trick for now. The data may have been removed before remote retrieving. 
        ret.mDatasetId = - ret.mDatasetId;
        ret.mDataLength = 0;
        ret.mData = ByteBuffer.allocate(0);
        ret.generateHeader();
        ret.LOG.error(e.getMessage(), e);
      }
    } else {
      ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
      ret.mData = null;
    }

    return ret;
  }

  public void close() {
    if (mMsgType == DATA_SERVER_RESPONSE_MESSAGE) {
      try {
        mFile.close();
      } catch (IOException e) {
        LOG.error(mFile + " " + e.getMessage());
      }
    }
  }

  private void generateHeader() {
    mHeader.clear();
    mHeader.putInt(mDatasetId);
    mHeader.putInt(mPartitionId);
    mHeader.putInt(mDataLength);
    mHeader.flip();
  }

  public int recv(SocketChannel socketChannel) throws IOException {
    isSend(false);

    int numRead = 0;

    if (mHeader.remaining() > 0) {
      numRead = socketChannel.read(mHeader);
      if (mHeader.remaining() == 0) {
        mHeader.flip();
        mDatasetId = mHeader.getInt();
        mPartitionId = mHeader.getInt();
        mDataLength = mHeader.getInt();
        mData = ByteBuffer.allocate(mDataLength);
        LOG.info("recv(): mData: " + mData);
        if (mDataLength == 0) {
          mIsMessageReady = true;
        }
      }
    } else {
      numRead = socketChannel.read(mData);
      if (mData.remaining() == 0) {
        mIsMessageReady = true;
      }
    }

    return numRead;
  }

  public void send(SocketChannel socketChannel) throws IOException {
    isSend(true);

    socketChannel.write(mHeader);

    if (mHeader.remaining() == 0) {
      socketChannel.write(mData);
    }
  }

  public boolean finishSending() {
    isSend(true);

    return mHeader.remaining() == 0 && mData.remaining() == 0;
  }

  private void isSend(boolean isSend) {
    if (IS_TO_SEND_DATA != isSend) {
      if (IS_TO_SEND_DATA) {
        CommonUtils.runtimeException("Try to recv on send message");
      } else {
        CommonUtils.runtimeException("Try to send on recv message");
      }
    }
  }

  public boolean isMessageReady() {
    return mIsMessageReady;
  }

  public int getDatasetId() {
    if (!mIsMessageReady) {
      CommonUtils.runtimeException("Message is not ready.");
    }
    return mDatasetId;
  }

  public int getPartitionId() {
    if (!mIsMessageReady) {
      CommonUtils.runtimeException("Message is not ready.");
    }
    return mPartitionId;
  }

  public ByteBuffer getReadOnlyData() {
    if (!mIsMessageReady) {
      CommonUtils.runtimeException("Message is not ready.");
    }
    ByteBuffer ret = ByteBuffer.wrap(mData.array());
    ret.asReadOnlyBuffer();
    return ret;
  }
}
