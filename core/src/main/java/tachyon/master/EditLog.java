package tachyon.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.UnderFileSystem;
import tachyon.io.Utils;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableDoesNotExistException;
import tachyon.thrift.TachyonException;
import tachyon.util.CommonUtils;

/**
 * Master operation journal.
 */
public class EditLog {
  private final static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static int mBackUpLogStartNum = -1;

  private static long mCurrentTId = 0;

  /**
   * Load edit log.
   * 
   * @param info
   *          The Master Info.
   * @param path
   *          The path of the edit logs.
   * @param currentLogFileNum
   *          The smallest completed log number that this master has not loaded
   * @return The last transaction id.
   * @throws IOException
   */
  public static long load(MasterInfo info, String path, int currentLogFileNum) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    try {
      if (!ufs.exists(path)) {
        LOG.info("Edit Log " + path + " does not exist.");
        return 0;
      }
      LOG.info("currentLogNum passed in was " + currentLogFileNum);
      int completedLogs = currentLogFileNum;
      mBackUpLogStartNum = currentLogFileNum;
      int numFiles = 1;
      String completedPath =
          path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR) + 1) + "completed";
      if (!ufs.exists(completedPath)) {
        LOG.info("No completed edit logs to be parsed");
      } else {
        while (ufs.exists(CommonUtils.concat(completedPath, (completedLogs ++) + ".editLog"))) {
          numFiles ++;
        }
      }
      String editLogs[] = new String[numFiles];
      for (int i = 0; i < numFiles; i ++) {
        if (i != numFiles - 1) {
          editLogs[i] = CommonUtils.concat(completedPath, (i + currentLogFileNum) + ".editLog");
        } else {
          editLogs[i] = path;
        }
      }

      for (String currentPath : editLogs) {
        LOG.info("Loading Edit Log " + currentPath);
        loadSingleLog(info, currentPath);
      }
      return mCurrentTId;
    } finally {
      ufs.close();
    }
  }

  /**
   * Load one edit log.
   * 
   * @param info
   *          The Master Info
   * @param path
   *          The path of the edit log
   * @throws IOException
   */
  public static void loadSingleLog(MasterInfo info, String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    DataInputStream is = null;
    try {
      is = new DataInputStream(ufs.open(path));
      JsonParser parser = JsonObject.createObjectMapper().getFactory().createParser(is);

      while (true) {
        EditLogOperation op;
        try {
          op = parser.readValueAs(EditLogOperation.class);
          LOG.debug("Read operation: " + op);
        } catch (IOException e) {
          // Unfortunately brittle, but Jackson rethrows EOF with this message.
          if (e.getMessage().contains("end-of-input")) {
            break;
          } else {
            throw e;
          }
        }

        mCurrentTId = op.transId;
        try {
          switch (op.type) {
          case ADD_BLOCK: {
            info.opAddBlock(op.getInt("fileId"), op.getInt("blockIndex"),
                op.getLong("blockLength"), op.getLong("opTimeMs"));
            break;
          }
          case ADD_CHECKPOINT: {
            info._addCheckpoint(-1, op.getInt("fileId"), op.getLong("length"),
                op.getString("path"), op.getLong("opTimeMs"));
            break;
          }
          case CREATE_FILE: {
            info._createFile(op.getBoolean("recursive"), op.getString("path"),
                op.getBoolean("directory"), op.getLong("blockSizeByte"),
                op.getLong("creationTimeMs"));
            break;
          }
          case COMPLETE_FILE: {
            info._completeFile(op.<Integer> get("fileId"), op.getLong("opTimeMs"));
            break;
          }
          case SET_PINNED: {
            info._setPinned(op.getInt("fileId"), op.getBoolean("pinned"), op.getLong("opTimeMs"));
            break;
          }
          case RENAME: {
            info._rename(op.getInt("fileId"), op.getString("dstPath"), op.getLong("opTimeMs"));
            break;
          }
          case DELETE: {
            info._delete(op.getInt("fileId"), op.getBoolean("recursive"), op.getLong("opTimeMs"));
            break;
          }
          case CREATE_RAW_TABLE: {
            info._createRawTable(op.getInt("tableId"), op.getInt("columns"),
                op.getByteBuffer("metadata"));
            break;
          }
          case UPDATE_RAW_TABLE_METADATA: {
            info.updateRawTableMetadata(op.getInt("tableId"), op.getByteBuffer("metadata"));
            break;
          }
          case CREATE_DEPENDENCY: {
            info._createDependency(op.<List<Integer>> get("parents"),
                op.<List<Integer>> get("children"), op.getString("commandPrefix"),
                op.getByteBufferList("data"), op.getString("comment"), op.getString("framework"),
                op.getString("frameworkVersion"), op.<DependencyType> get("dependencyType"),
                op.getInt("dependencyId"), op.getLong("creationTimeMs"));
            break;
          }
          default:
            throw new IOException("Invalid op type " + op);
          }
        } catch (SuspectedFileSizeException e) {
          throw new IOException(e);
        } catch (BlockInfoException e) {
          throw new IOException(e);
        } catch (FileDoesNotExistException e) {
          throw new IOException(e);
        } catch (FileAlreadyExistException e) {
          throw new IOException(e);
        } catch (InvalidPathException e) {
          throw new IOException(e);
        } catch (TachyonException e) {
          throw new IOException(e);
        } catch (TableDoesNotExistException e) {
          throw new IOException(e);
        }
      }
    } finally {
      boolean ufsClosed = false;
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          ufs.close();
          ufsClosed = true;
          // TODO: Do we need to throw this IOException if ufs.close() succeeded?
          // We can save this IOException and throw after line # 220
        }
      }
      if (!ufsClosed) {
        ufs.close();
      }
    }
  }

  /**
   * Make the edit log up-to-date, It will delete all editlogs since mBackUpLogStartNum.
   * 
   * @param path
   *          The path of the edit logs
   */
  public static void markUpToDate(String path) {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    String folder =
        path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR) + 1) + "completed";
    try {
      // delete all loaded editlogs since mBackupLogStartNum.
      String toDelete = CommonUtils.concat(folder, mBackUpLogStartNum + ".editLog");
      while (ufs.exists(toDelete)) {
        LOG.info("Deleting editlog " + toDelete);
        ufs.delete(toDelete, true);
        mBackUpLogStartNum ++;
        toDelete = CommonUtils.concat(folder, mBackUpLogStartNum + ".editLog");
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    mBackUpLogStartNum = -1;
  }

  /** When a master is replaying an edit log, mark the current edit log as an INACTIVE one. */
  private final boolean INACTIVE;

  private final String PATH;

  /** Writer used to serialize Operations into the edit log. */
  private final ObjectWriter WRITER;

  private UnderFileSystem mUfs;

  /** Raw output stream to the UnderFS */
  private OutputStream mOs;

  /** Wraps the raw output stream. */
  private DataOutputStream mDos;

  // Starting from 1.
  private long mFlushedTransactionId = 0;

  private long mTransactionId = 0;

  private int mCurrentLogFileNum = 0;

  private int mMaxLogSize = 5 * Constants.MB;

  /**
   * Create a new EditLog
   * 
   * @param path
   *          The path of the edit logs.
   * @param inactive
   *          If a master is replaying an edit log, the current edit log is inactive.
   * @param transactionId
   *          The beginning transactionId of the edit log
   * @throws IOException
   */
  public EditLog(String path, boolean inactive, long transactionId) throws IOException {
    INACTIVE = inactive;

    if (!INACTIVE) {
      LOG.info("Creating edit log file " + path);
      PATH = path;
      mUfs = UnderFileSystem.get(path);
      if (mBackUpLogStartNum != -1) {
        String folder =
            path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR) + 1) + "/completed";
        LOG.info("Deleting completed editlogs that are part of the image.");
        deleteCompletedLogs(path, mBackUpLogStartNum);
        LOG.info("Backing up logs from " + mBackUpLogStartNum + " since image is not updated.");
        mUfs.mkdirs(folder, true);
        String toRename = CommonUtils.concat(folder, mBackUpLogStartNum + ".editLog");
        int currentLogFileNum = 0;
        while (mUfs.exists(toRename)) {
          LOG.info("Rename " + toRename + " to "
              + CommonUtils.concat(folder, currentLogFileNum + ".editLog"));
          currentLogFileNum ++;
          mBackUpLogStartNum ++;
          toRename = CommonUtils.concat(folder, mBackUpLogStartNum + ".editLog");
        }
        if (mUfs.exists(path)) {
          mUfs.rename(path, CommonUtils.concat(folder, currentLogFileNum + ".editLog"));
          LOG.info("Rename " + path + " to "
              + CommonUtils.concat(folder, currentLogFileNum + ".editLog"));
          currentLogFileNum ++;
        }
        mBackUpLogStartNum = -1;
      }

      // In case this file is created by different dfs-clients, which has been
      // fixed in HDFS-3755 since 3.0.0, 2.0.2-alpha
      if (mUfs.exists(path)) {
        mUfs.delete(path, true);
      }
      mOs = mUfs.create(path);
      mDos = new DataOutputStream(mOs);
      LOG.info("Created file " + path);
      mFlushedTransactionId = transactionId;
      mTransactionId = transactionId;
      WRITER = JsonObject.createObjectMapper().writer();
    } else {
      PATH = null;
      mUfs = null;
      mOs = null;
      mDos = null;
      WRITER = null;
    }
  }

  /**
   * Only close the currently opened output streams.
   */
  private synchronized void _closeActiveStream() {
    try {
      if (mDos != null) {
        mDos.close();
      }
      if (mOs != null) {
        mOs.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Log an addBlock operation. Do nothing if the edit log is inactive.
   * 
   * @param fileId
   *          The id of the file
   * @param blockIndex
   *          The index of the block to be added
   * @param blockLength
   *          The length of the block to be added
   * @param opTimeMs
   *          The time of the addBlock operation, in milliseconds
   */
  public synchronized void addBlock(int fileId, int blockIndex, long blockLength, long opTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.ADD_BLOCK, ++ mTransactionId)
            .withParameter("fileId", fileId).withParameter("blockIndex", blockIndex)
            .withParameter("blockLength", blockLength).withParameter("opTimeMs", opTimeMs);
    writeOperation(operation);
  }

  /**
   * Log an addCheckpoint operation. Do nothing if the edit log is inactive.
   * 
   * @param fileId
   *          The file to add the checkpoint
   * @param length
   *          The length of the checkpoint
   * @param checkpointPath
   *          The path of the checkpoint
   * @param opTimeMs
   *          The time of the addCheckpoint operation, in milliseconds
   */
  public synchronized void addCheckpoint(int fileId, long length, String checkpointPath,
      long opTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.ADD_CHECKPOINT, ++ mTransactionId)
            .withParameter("fileId", fileId).withParameter("length", length)
            .withParameter("path", checkpointPath).withParameter("opTimeMs", opTimeMs);
    writeOperation(operation);
  }

  /**
   * Close the log.
   */
  public synchronized void close() {
    if (INACTIVE) {
      return;
    }

    try {
      _closeActiveStream();
      mUfs.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Log a completeFile operation. Do nothing if the edit log is inactive.
   * 
   * @param fileId
   *          The id of the file
   * @param opTimeMs
   *          The time of the completeFile operation, in milliseconds
   */
  public synchronized void completeFile(int fileId, long opTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.COMPLETE_FILE, ++ mTransactionId).withParameter(
            "fileId", fileId).withParameter("opTimeMs", opTimeMs);
    writeOperation(operation);
  }

  /**
   * Log a createDependency operation. The parameters are like creating a new Dependency. Do nothing
   * if the edit log is inactive.
   * 
   * @param parents
   *          The input files' id of the dependency
   * @param children
   *          The output files' id of the dependency
   * @param commandPrefix
   *          The prefix of the command used for recomputation
   * @param data
   *          The list of the data used for recomputation
   * @param comment
   *          The comment of the dependency
   * @param framework
   *          The framework of the dependency, used for recomputation
   * @param frameworkVersion
   *          The version of the framework
   * @param dependencyType
   *          The type of the dependency, DependencyType.Wide or DependencyType.Narrow
   * @param depId
   *          The id of the dependency
   * @param creationTimeMs
   *          The create time of the dependency, in milliseconds
   */
  public synchronized void createDependency(List<Integer> parents, List<Integer> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, DependencyType dependencyType, int depId, long creationTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.CREATE_DEPENDENCY, ++ mTransactionId)
            .withParameter("parents", parents).withParameter("children", children)
            .withParameter("commandPrefix", commandPrefix)
            .withParameter("data", Utils.byteBufferListToBase64(data))
            .withParameter("comment", comment).withParameter("framework", framework)
            .withParameter("frameworkVersion", frameworkVersion)
            .withParameter("dependencyType", dependencyType).withParameter("dependencyId", depId)
            .withParameter("creationTimeMs", creationTimeMs);
    writeOperation(operation);
  }

  /**
   * Log a createFile operation. Do nothing if the edit log is inactive.
   * 
   * @param recursive
   *          If recursive is true and the filesystem tree is not filled in all the way to path yet,
   *          it fills in the missing components.
   * @param path
   *          The path to create
   * @param directory
   *          If true, creates an InodeFolder instead of an Inode
   * @param blockSizeByte
   *          If it's a file, the block size for the Inode
   * @param creationTimeMs
   *          The time the file was created
   */
  public synchronized void createFile(boolean recursive, String path, boolean directory,
      long blockSizeByte, long creationTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.CREATE_FILE, ++ mTransactionId)
            .withParameter("recursive", recursive).withParameter("path", path)
            .withParameter("directory", directory).withParameter("blockSizeByte", blockSizeByte)
            .withParameter("creationTimeMs", creationTimeMs);
    writeOperation(operation);
  }

  /**
   * Log a createRawTable operation. Do nothing if the edit log is inactive.
   * 
   * @param tableId
   *          The id of the raw table
   * @param columns
   *          The number of columns in the table
   * @param metadata
   *          Additional metadata about the table
   */
  public synchronized void createRawTable(int tableId, int columns, ByteBuffer metadata) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.CREATE_RAW_TABLE, ++ mTransactionId)
            .withParameter("tableId", tableId).withParameter("columns", columns)
            .withParameter("metadata", Utils.byteBufferToBase64(metadata));
    writeOperation(operation);
  }

  /**
   * Log a delete operation. Do nothing if the edit log is inactive.
   * 
   * @param fileId
   *          the file to be deleted.
   * @param recursive
   *          whether delete the file recursively or not.
   * @param opTimeMs
   *          The time of the delete operation, in milliseconds
   */
  public synchronized void delete(int fileId, boolean recursive, long opTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.DELETE, ++ mTransactionId)
            .withParameter("fileId", fileId).withParameter("recursive", recursive)
            .withParameter("opTimeMs", opTimeMs);
    writeOperation(operation);
  }

  /**
   * Delete the completed logs.
   * 
   * @param path
   *          The path of the logs
   * @param upTo
   *          The logs in the path from 0 to upTo-1 are completed and to be deleted
   */
  public void deleteCompletedLogs(String path, int upTo) {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    String folder =
        path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR) + 1) + "completed";
    try {
      for (int i = 0; i < upTo; i ++) {
        String toDelete = CommonUtils.concat(folder, i + ".editLog");
        LOG.info("Deleting editlog " + toDelete);
        ufs.delete(toDelete, true);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Flush the log onto the storage.
   */
  public synchronized void flush() {
    if (INACTIVE) {
      return;
    }

    try {
      mDos.flush();
      if (mOs instanceof FSDataOutputStream) {
        ((FSDataOutputStream) mOs).sync();
      }
      if (mDos.size() > mMaxLogSize) {
        rotateEditLog(PATH);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    mFlushedTransactionId = mTransactionId;
  }

  /**
   * Get the current TransactionId and FlushedTransactionId
   * 
   * @return (TransactionId, FlushedTransactionId)
   */
  public synchronized Pair<Long, Long> getTransactionIds() {
    return new Pair<Long, Long>(mTransactionId, mFlushedTransactionId);
  }

  /**
   * Log a rename operation. Do nothing if the edit log is inactive.
   * 
   * @param fileId
   *          The id of the file to rename
   * @param dstPath
   *          The new path of the file
   * @param opTimeMs
   *          The time of the rename operation, in milliseconds
   */
  public synchronized void rename(int fileId, String dstPath, long opTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.RENAME, ++ mTransactionId)
            .withParameter("fileId", fileId).withParameter("dstPath", dstPath)
            .withParameter("opTimeMs", opTimeMs);
    writeOperation(operation);
  }

  /**
   * The edit log reaches the max log size and needs rotate. Do nothing if the edit log is inactive.
   * 
   * @param path
   *          The path of the edit log
   */
  public void rotateEditLog(String path) {
    if (INACTIVE) {
      return;
    }

    _closeActiveStream();
    LOG.info("Edit log max size of " + mMaxLogSize + " bytes reached, rotating edit log");
    String pathPrefix =
        path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR) + 1) + "completed";
    LOG.info("path: " + path + " prefix: " + pathPrefix);
    try {
      if (!mUfs.exists(pathPrefix)) {
        mUfs.mkdirs(pathPrefix, true);
      }
      String newPath = CommonUtils.concat(pathPrefix, (mCurrentLogFileNum ++) + ".editLog");
      mUfs.rename(path, newPath);
      LOG.info("Renamed " + path + " to " + newPath);
      mOs = mUfs.create(path);
      mDos = new DataOutputStream(mOs);
      LOG.info("Created new log file " + path);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Changes the max log size for testing purposes.
   * 
   * @param size
   */
  public void setMaxLogSize(int size) {
    mMaxLogSize = size;
  }

  /**
   * Log a setPinned operation. Do nothing if the edit log is inactive.
   * 
   * @param fileId
   *          The id of the file
   * @param pinned
   *          If true, the file is never evicted from memory
   * @param opTimeMs
   *          The time of the setPinned operation, in milliseconds
   */
  public synchronized void setPinned(int fileId, boolean pinned, long opTimeMs) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.SET_PINNED, ++ mTransactionId)
            .withParameter("fileId", fileId).withParameter("pinned", pinned)
            .withParameter("opTimeMs", opTimeMs);
    writeOperation(operation);
  }

  /**
   * Log an updateRawTableMetadata operation. Do nothing if the edit log is inactive.
   * 
   * @param tableId
   *          The id of the raw table
   * @param metadata
   *          The new metadata of the raw table
   */
  public synchronized void updateRawTableMetadata(int tableId, ByteBuffer metadata) {
    if (INACTIVE) {
      return;
    }

    EditLogOperation operation =
        new EditLogOperation(EditLogOperationType.UPDATE_RAW_TABLE_METADATA, ++ mTransactionId)
            .withParameter("tableId", tableId).withParameter("metadata",
                Utils.byteBufferToBase64(metadata));
    writeOperation(operation);
  }

  private void writeOperation(EditLogOperation operation) {
    try {
      WRITER.writeValue(mDos, operation);
      mDos.writeByte('\n');
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
