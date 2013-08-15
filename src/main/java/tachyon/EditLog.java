package tachyon;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.io.Utils;

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

  private final static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  // When a master is replaying an edit log, make the current edit log as an INACTIVE one.
  private final boolean INACTIVE;
  private final DataOutputStream DOS;

  public static void load(MasterInfo info, String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    if (!ufs.exists(path)) {
      LOG.info("Edit Log " + path + " does not exist.");
      return;
    }
    LOG.info("Loading Edit Log " + path);

    DataInputStream is = new DataInputStream(ufs.open(path));

    while (true) {
      byte op;
      try {
        op = is.readByte();
      } catch (EOFException e) {
        break;
      }

      switch (op) {
      case OP_ADD_CHECKPOINT: {
        break;
      }
      case OP_ADD_BLOCK: {
        break;
      }
      case OP_CREATE_FILE: {
        break;
      }
      case OP_DELETE: {
        break;
      }
      case OP_RENAME: {
        break;
      }
      case OP_UNPIN_FILE: {
        break;
      }
      case OP_UPDATE_RAW_TABLE_METADATA: {
        break;
      }
      default :
        throw new IOException("Invalid op type " + op);
      }
    }

    is.close();
  }

  public EditLog(String path, boolean inactive) throws IOException {
    INACTIVE = inactive;

    if (!INACTIVE) {
      LOG.info("Creating edit log file " + path);
      UnderFileSystem ufs = UnderFileSystem.get(path);
      DOS = new DataOutputStream(ufs.create(path));
    } else {
      DOS = null;
    }
  }

  public synchronized void flush() {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.flush();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

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

  public synchronized void addCheckpoint(int fileId, long length, String checkpointPath) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeByte(OP_ADD_CHECKPOINT);
      DOS.writeInt(fileId);
      DOS.writeLong(length);
      Utils.writeString(checkpointPath, DOS);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void addBlock(int fileId, int blockIndex, long blockLength) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeByte(OP_ADD_BLOCK);
      DOS.writeInt(fileId);
      DOS.writeInt(blockIndex);
      DOS.writeLong(blockLength);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void createFile(boolean recursive, String path, boolean directory, int columns,
      ByteBuffer metadata, long blockSizeByte) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeByte(OP_CREATE_FILE);
      DOS.writeBoolean(recursive);
      Utils.writeString(path, DOS);
      DOS.writeBoolean(directory);
      DOS.writeInt(columns);
      if (columns != -1) {
        DOS.writeInt(metadata.limit() - metadata.position());
        DOS.write(metadata.array(), metadata.position(), metadata.limit() - metadata.position());
      }
      DOS.writeLong(blockSizeByte);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void delete(int fileId, boolean recursive) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeByte(OP_DELETE);
      DOS.writeInt(fileId);
      DOS.writeBoolean(recursive);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void rename(int fileId, String dstPath) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeByte(OP_RENAME);
      DOS.writeInt(fileId);
      Utils.writeString(dstPath, DOS);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void unpinFile(int fileId) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeByte(OP_UNPIN_FILE);
      DOS.writeInt(fileId);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void updateRawTableMetadata(int tableId, ByteBuffer metadata) {
    if (INACTIVE) {
      return;
    }

    try {
      DOS.writeByte(OP_UPDATE_RAW_TABLE_METADATA);
      DOS.writeInt(tableId);
      Utils.writeByteBuffer(metadata, DOS);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }
}
