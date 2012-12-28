package tachyon.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import tachyon.CommonUtils;
import tachyon.thrift.OutOfMemoryForPinDatasetException;

/**
 * RawColumnDatasetPartition
 * @author Haoyuan
 */
public class RCDPartition {
  private final TachyonClient mTachyonClient;
  private final RawColumnDataset mRawColumnDataset;
  private final int mDatasetId;
  private final int mPartitionId;

  private ArrayList<Partition> mColumnPartitions = new ArrayList<Partition>();

  private boolean mOpen = false;
  private boolean mRead;
  private boolean mWriteThrough;

  public RCDPartition(TachyonClient tachyonClient, RawColumnDataset rawColumnDataset, 
      int datasetId, int pId) {
    mTachyonClient = tachyonClient;
    mRawColumnDataset = rawColumnDataset;
    mDatasetId = datasetId;
    mPartitionId = pId;
    mColumnPartitions = new ArrayList<Partition>(mRawColumnDataset.getNumColumns());
  }

  public void open(String wr) throws IOException {
    open(wr, true);
  }

  public void open(String wr, boolean writeThrough) throws IOException {
    if (wr.equals("r")) {
      mRead = true;
    } else if (wr.equals("w")) {
      mRead = false;
      mWriteThrough = writeThrough;
    } else {
      CommonUtils.runtimeException("Wrong option to open a partition: " + wr);
    }

    for (int k = 0; k < mRawColumnDataset.getNumColumns(); k ++) {
      Dataset tDataset = mTachyonClient.getDataset(mRawColumnDataset.getColumnDatasetIds().get(k));
      Partition tPartition = tDataset.getPartition(mPartitionId);
      tPartition.open(wr, mWriteThrough);
      mColumnPartitions.add(tPartition);
    }

    mOpen = true;
  }

  public void append(int columnId, byte b) throws IOException {
    validateIO(false);
    validateColumnId(columnId);
    mColumnPartitions.get(columnId).append(b);
  }

  public void append(int columnId, int b) throws IOException {
    validateIO(false);
    validateColumnId(columnId);
    mColumnPartitions.get(columnId).append(b);
  }

  public void append(int columnId, byte[] buf, int off, int len) 
      throws IOException, OutOfMemoryForPinDatasetException {
    validateIO(false);
    validateColumnId(columnId);
    mColumnPartitions.get(columnId).append(buf, off, len);
  }

  public void append(int columnId, ByteBuffer buf) throws IOException, OutOfMemoryForPinDatasetException {
    append(columnId, buf.array(), buf.position(), buf.limit() - buf.position());
  }

  public void append(int columnId, ArrayList<ByteBuffer> bufs) 
      throws IOException, OutOfMemoryForPinDatasetException {
    for (int k = 0; k < bufs.size(); k ++) {
      append(columnId, bufs.get(k));
    }
  }

  public void close() {
    if (! mOpen) {
      return;
    }

    int sizeBytes = 0;
    for (int k = 0; k < mRawColumnDataset.getNumColumns(); k ++) {
      mColumnPartitions.get(k).close();
      sizeBytes += mColumnPartitions.get(k).getSize();
    }

    mTachyonClient.addDoneRCDPartition(mDatasetId, mPartitionId, sizeBytes);

    mOpen = false;
  }

  public ByteBuffer readByteBuffer(int columnId)
      throws UnknownHostException, FileNotFoundException, IOException {
    validateIO(true);
    validateColumnId(columnId);

    return mColumnPartitions.get(columnId).readByteBuffer();
  }

  private void validateColumnId(int columnId) {
    if (columnId < 0 || columnId >= mRawColumnDataset.getNumColumns()) {
      CommonUtils.runtimeException(mRawColumnDataset.getPath() + " does not have column " + columnId
          + ". It has " + mRawColumnDataset.getNumColumns() + " columns.");
    }
  }

  private void validateIO(boolean read) {
    if (! mOpen) {
      CommonUtils.runtimeException("The partition " + mDatasetId + "-" + mPartitionId + 
          "was never openned or has been closed.");
    }
    if (read != mRead) {
      CommonUtils.illegalArgumentException("The partition " + mDatasetId + "-" + mPartitionId + "" +
          "was opened for " + (mRead ? "Read" : "Write") + ". " + 
          (read ? "Read" : "Write") + " operation is not available.");
    }
  }
}