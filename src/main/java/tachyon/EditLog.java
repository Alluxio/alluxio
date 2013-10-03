package tachyon;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.io.Utils;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableDoesNotExistException;
import tachyon.thrift.TachyonException;

/**
 * Master operation journal.
 */
public class EditLog {
  static final byte OP_INVALID = -1;
  static final byte OP_ADD_CHECKPOINT = 0;
  static final byte OP_ADD_BLOCK = 1;
  static final byte OP_CREATE_FILE = 2;
  static final byte OP_DELETE = 3;
  static final byte OP_RENAME = 4;
  static final byte OP_UNPIN_FILE = 5;
  static final byte OP_UPDATE_RAW_TABLE_METADATA = 6;
  static final byte OP_COMPLETE_FILE = 7;

  private final static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  // When a master is replaying an edit log, make the current edit log as an INACTIVE one.
  private final boolean INACTIVE;
  private final DataOutputStream DOS;

  // Starting from 1.
  private long mFlushedTransactionId = 0;
  private long mTransactionId = 0;

  /**
   * Load edit log.
   * @param info The Master Info.
   * @return The last transaction id.
   * @throws IOException
   */
  public static long load(MasterInfo info, String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    if (!ufs.exists(path)) {
      LOG.info("Edit Log " + path + " does not exist.");
      return 0;
    }
    LOG.info("Loading Edit Log " + path);

    DataInputStream is = new DataInputStream(ufs.open(path));
    long transactionId = 0;

    while (true) {
      byte op;
      long tId;
      try {
        tId = is.readLong();
      } catch (EOFException e) {
        break;
      }

      transactionId = tId;
      op = is.readByte();
      try {
        switch (op) {
        case OP_ADD_CHECKPOINT: {
          info.addCheckpoint(-1, is.readInt(), is.readLong(), Utils.readString(is));
          break;
        }
        case OP_ADD_BLOCK: {
          info.opAddBlock(is.readInt(), is.readInt(), is.readLong());
          break;
        }
        case OP_CREATE_FILE: {
          info._createFile(is.readBoolean(), Utils.readString(is), is.readBoolean(), is.readInt(), 
	      Utils.readByteBuffer(is), is.readLong(), is.readLong(), is.readBoolean());
          break;
        }
        case OP_DELETE: {
          info.delete(is.readInt(), is.readBoolean());
          break;
        }
        case OP_RENAME: {
          info.rename(is.readInt(), Utils.readString(is));
          break;
        }
        case OP_UNPIN_FILE: {
          info.unpinFile(is.readInt());
          break;
        }
        case OP_UPDATE_RAW_TABLE_METADATA: {
          info.updateRawTableMetadata(is.readInt(), Utils.readByteBuffer(is));
          break;
        }
        case OP_COMPLETE_FILE: {
          info.completeFile(is.readInt());
          break;
        }
        default :
          throw new IOException("Invalid op type " + op);
        }
      } catch (SuspectedFileSizeException | BlockInfoException | FileDoesNotExistException |
          FileAlreadyExistException | InvalidPathException | TachyonException |
          TableDoesNotExistException e) {
        throw new IOException(e);
      }
    }

    is.close();
    return transactionId;
  }

  public EditLog(String path, boolean inactive, long transactionId) throws IOException {
    INACTIVE = inactive;

    if (!INACTIVE) {
      LOG.info("Creating edit log file " + path);
      UnderFileSystem ufs = UnderFileSystem.get(path);
      DOS = new DataOutputStream(ufs.create(path));
      mFlushedTransactionId = transactionId;
      mTransactionId = transactionId;
    } else {
      DOS = null;
    }
  }

  public synchronized void addCheckpoint(int fileId, long length, String checkpointPath) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_ADD_CHECKPOINT);
      DOS.writeInt(fileId);
      DOS.writeLong(length);
      Utils.writeString(checkpointPath, DOS);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void addBlock(int fileId, int blockIndex, long blockLength) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_ADD_BLOCK);
      DOS.writeInt(fileId);
      DOS.writeInt(blockIndex);
      DOS.writeLong(blockLength);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void createFile(boolean recursive, String path, boolean directory,
      int columns, ByteBuffer metadata, long blockSizeByte, long creationTimeMs, 
        boolean transparent) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_CREATE_FILE);
      DOS.writeBoolean(recursive);
      Utils.writeString(path, DOS);
      DOS.writeBoolean(directory);
      DOS.writeInt(columns);
      Utils.writeByteBuffer(metadata, DOS);
      DOS.writeLong(blockSizeByte);
      DOS.writeLong(creationTimeMs);
      DOS.writeBoolean(transparent);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void delete(int fileId, boolean recursive) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_DELETE);
      DOS.writeInt(fileId);
      DOS.writeBoolean(recursive);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void rename(int fileId, String dstPath) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_RENAME);
      DOS.writeInt(fileId);
      Utils.writeString(dstPath, DOS);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void unpinFile(int fileId) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_UNPIN_FILE);
      DOS.writeInt(fileId);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void updateRawTableMetadata(int tableId, ByteBuffer metadata) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_UPDATE_RAW_TABLE_METADATA);
      DOS.writeInt(tableId);
      Utils.writeByteBuffer(metadata, DOS);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void completeFile(int fileId) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeLong(++ mTransactionId);
      DOS.writeByte(OP_COMPLETE_FILE);
      DOS.writeInt(fileId);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  /**
   * Get the current TransactionId and FlushedTransactionId
   * @return (TransactionId, FlushedTransactionId)
   */
  public synchronized Pair<Long, Long> getTransactionIds() {
    return new Pair<Long, Long>(mTransactionId, mFlushedTransactionId);
  }

  /**
   * Flush the log onto the storage.
   */
  public synchronized void flush() {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.flush();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }

    mFlushedTransactionId = mTransactionId;
  }

  /**
   * Close the log.
   */
  public synchronized void close() {
    if (INACTIVE) {
      return;
    }
    try {
      DOS.close();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }
}
