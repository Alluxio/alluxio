/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;

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

  // When a master is replaying an edit log, mark the current edit log as an INACTIVE one.
  private final boolean INACTIVE;

  private final String PATH;

  /** Writer used to serialize Operations into the edit log. */
  private final ObjectWriter writer;

  private UnderFileSystem UFS;
  private DataOutputStream DOS;
  private OutputStream OS;

  // Starting from 1.
  private long mFlushedTransactionId = 0;

  private long mTransactionId = 0;

  private int mCurrentLogFileNum = 0;

  private int mMaxLogSize = 5 * Constants.MB;

  public EditLog(String path, boolean inactive, long transactionId) throws IOException {
    INACTIVE = inactive;

    if (!INACTIVE) {
      LOG.info("Creating edit log file " + path);
      PATH = path;
      UFS = UnderFileSystem.get(path);
      if (mBackUpLogStartNum != -1) {
        String folder =
            path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR) + 1) + "/completed";
        LOG.info("Deleting completed editlogs that are part of the image.");
        deleteCompletedLogs(path, mBackUpLogStartNum);
        LOG.info("Backing up logs from " + mBackUpLogStartNum + " since image is not updated.");
        UFS.mkdirs(folder, true);
        String toRename = CommonUtils.concat(folder, mBackUpLogStartNum + ".editLog");
        int currentLogFileNum = 0;
        while (UFS.exists(toRename)) {
          LOG.info("Rename " + toRename + " to "
              + CommonUtils.concat(folder, currentLogFileNum + ".editLog"));
          currentLogFileNum ++;
          mBackUpLogStartNum ++;
          toRename = CommonUtils.concat(folder, mBackUpLogStartNum + ".editLog");
        }
        if (UFS.exists(path)) {
          UFS.rename(path, CommonUtils.concat(folder, currentLogFileNum + ".editLog"));
          LOG.info("Rename " + path + " to "
              + CommonUtils.concat(folder, currentLogFileNum + ".editLog"));
          currentLogFileNum ++;
        }
        mBackUpLogStartNum = -1;
      }

      // In case this file is created by different dfs-clients, which has been
      // fixed in HDFS-3755 since 3.0.0, 2.0.2-alpha
      if (UFS.exists(path)) {
        UFS.delete(path, true);
      }
      OS = UFS.create(path);
      DOS = new DataOutputStream(OS);
      LOG.info("Created file " + path);
      mFlushedTransactionId = transactionId;
      mTransactionId = transactionId;
      writer = createObjectMapper().writer();
    } else {
      PATH = null;
      UFS = null;
      OS = null;
      DOS = null;
      writer = null;
    }
  }

  /** Creates a JSON ObjectMapper configured not to close the underlying stream. */
  private static ObjectMapper createObjectMapper() {
    // TODO: Could disable field name quoting, though this would produce technically invalid JSON
    // See: JsonGenerator.QUOTE_FIELD_NAMES and JsonParser.ALLOW_UNQUOTED_FIELD_NAMES
    return new ObjectMapper()
        .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
        .configure(SerializationFeature.CLOSE_CLOSEABLE, false);
  }

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
    ufs.close();
    return mCurrentTId;
  }

  public static void loadSingleLog(MasterInfo info, String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    DataInputStream is = new DataInputStream(ufs.open(path));
    JsonParser parser = createObjectMapper().getJsonFactory().createJsonParser(is);

    while (true) {
      Operation op;
      try {
        op = parser.readValueAs(Operation.class);
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
            info.opAddBlock(
                op.getInt("fileId"),
                op.getInt("blockIndex"),
                op.getLong("blockLength"));
            break;
          }
          case ADD_CHECKPOINT: {
            info.addCheckpoint(
                -1,
                op.getInt("fileId"),
                op.getLong("length"),
                op.getString("path"));
            break;
          }
          case CREATE_FILE: {
            info._createFile(
                op.getBoolean("recursive"),
                op.getString("path"),
                op.getBoolean("directory"),
                op.getLong("blockSizeByte"),
                op.getLong("creationTimeMs"));
            break;
          }
          case COMPLETE_FILE: {
            info.completeFile(op.<Integer>get("fileId"));
            break;
          }
          case SET_PINNED: {
            info.setPinned(
                op.getInt("fileId"),
                op.getBoolean("pinned"));
            break;
          }
          case RENAME: {
            info._rename(
                op.getInt("fileId"),
                op.getString("dstPath"));
            break;
          }
          case DELETE: {
            info._delete(
                op.getInt("fileId"),
                op.getBoolean("recursive"));
            break;
          }
          case CREATE_RAW_TABLE: {
            info._createRawTable(
                op.getInt("tableId"),
                op.getInt("columns"),
                op.getByteBuffer("metadata"));
            break;
          }
          case UPDATE_RAW_TABLE_METADATA: {
            info.updateRawTableMetadata(
                op.getInt("tableId"),
                op.getByteBuffer("metadata"));
            break;
          }
          case CREATE_DEPENDENCY: {
            info._createDependency(
                op.<List<Integer>>get("parents"),
                op.<List<Integer>>get("children"),
                op.getString("commandPrefix"),
                op.getByteBufferList("data"),
                op.getString("comment"),
                op.getString("framework"),
                op.getString("frameworkVersion"),
                op.<DependencyType>get("dependencyType"),
                op.getInt("dependencyId"),
                op.getLong("creationTimeMs"));
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

    is.close();
    ufs.close();
  }

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
      CommonUtils.runtimeException(e);
    }
    mBackUpLogStartNum = -1;
  }

  public static void deleteCompletedLogs(String path, int upTo) {
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
      CommonUtils.runtimeException(e);
    }
  }

  private void writeOperation(Operation operation) {
    try {
      writer.writeValue(DOS, operation);
      DOS.writeByte('\n');
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
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
      DOS.flush();
      if (OS instanceof FSDataOutputStream) {
        ((FSDataOutputStream) OS).sync();
      }
      if (DOS.size() > mMaxLogSize) {
        rotateEditLog(PATH);
      }
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
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

  public void rotateEditLog(String path) {
    if (INACTIVE) { return; }

    _closeActiveStream();
    LOG.info("Edit log max size of " + mMaxLogSize + " bytes reached, rotating edit log");
    String pathPrefix =
        path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR) + 1) + "completed";
    LOG.info("path: " + path + " prefix: " + pathPrefix);
    try {
      if (!UFS.exists(pathPrefix)) {
        UFS.mkdirs(pathPrefix, true);
      }
      String newPath = CommonUtils.concat(pathPrefix, (mCurrentLogFileNum ++) + ".editLog");
      UFS.rename(path, newPath);
      LOG.info("Renamed " + path + " to " + newPath);
      OS = UFS.create(path);
      DOS = new DataOutputStream(OS);
      LOG.info("Created new log file " + path);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  /**
   * Only close the currently opened output streams.
   */
  private synchronized void _closeActiveStream() {
    try {
      if (DOS != null) {
        DOS.close();
      }
      if (OS != null) {
        OS.close();
      }
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
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
   * Close the log.
   */
  public synchronized void close() {
    if (INACTIVE) { return; }

    try {
      _closeActiveStream();
      UFS.close();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void addBlock(int fileId, int blockIndex, long blockLength) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.ADD_BLOCK, ++ mTransactionId)
        .withParameter("fileId", fileId)
        .withParameter("blockIndex", blockIndex)
        .withParameter("blockLength", blockLength);
    writeOperation(operation);
  }

  public synchronized void addCheckpoint(int fileId, long length, String checkpointPath) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.ADD_CHECKPOINT, ++ mTransactionId)
        .withParameter("fileId", fileId)
        .withParameter("length", length)
        .withParameter("path", checkpointPath);
    writeOperation(operation);
  }

  public synchronized void createFile(boolean recursive, String path, boolean directory,
      long blockSizeByte, long creationTimeMs) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.CREATE_FILE, ++ mTransactionId)
        .withParameter("recursive", recursive)
        .withParameter("path", path)
        .withParameter("directory", directory)
        .withParameter("blockSizeByte", blockSizeByte)
        .withParameter("creationTimeMs", creationTimeMs);
    writeOperation(operation);
  }

  public synchronized void completeFile(int fileId) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.COMPLETE_FILE, ++ mTransactionId)
        .withParameter("fileId", fileId);
    writeOperation(operation);
  }

  public synchronized void setPinned(int fileId, boolean pinned) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.SET_PINNED, ++ mTransactionId)
        .withParameter("fileId", fileId)
        .withParameter("pinned", pinned);
    writeOperation(operation);
  }

  public synchronized void rename(int fileId, String dstPath) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.RENAME, ++ mTransactionId)
        .withParameter("fileId", fileId)
        .withParameter("dstPath", dstPath);
    writeOperation(operation);
  }

  public synchronized void delete(int fileId, boolean recursive) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.DELETE, ++ mTransactionId)
        .withParameter("fileId", fileId)
        .withParameter("recursive", recursive);
    writeOperation(operation);
  }

  public synchronized void createRawTable(int tableId, int columns, ByteBuffer metadata) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.CREATE_RAW_TABLE, ++ mTransactionId)
        .withParameter("tableId", tableId)
        .withParameter("columns", columns)
        .withParameter("metadata", Utils.byteBufferToBase64(metadata));
    writeOperation(operation);
  }

  public synchronized void updateRawTableMetadata(int tableId, ByteBuffer metadata) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.UPDATE_RAW_TABLE_METADATA, ++ mTransactionId)
        .withParameter("tableId", tableId)
        .withParameter("metadata", Utils.byteBufferToBase64(metadata));
    writeOperation(operation);
  }

  public synchronized void createDependency(
      List<Integer> parents, List<Integer> children, String commandPrefix, List<ByteBuffer> data,
      String comment, String framework, String frameworkVersion, DependencyType dependencyType,
      int depId, long creationTimeMs) {
    if (INACTIVE) { return; }

    Operation operation = new Operation(OperationType.CREATE_DEPENDENCY, ++ mTransactionId)
        .withParameter("parents", parents)
        .withParameter("children", children)
        .withParameter("commandPrefix", commandPrefix)
        .withParameter("data", Utils.byteBufferListToBase64(data))
        .withParameter("comment", comment)
        .withParameter("framework", framework)
        .withParameter("frameworkVersion", frameworkVersion)
        .withParameter("dependencyType", dependencyType)
        .withParameter("dependencyId", depId)
        .withParameter("creationTimeMs", creationTimeMs);
    writeOperation(operation);
  }

  /** Type of EditLog entry. */
  private static enum OperationType {
    ADD_BLOCK,
    ADD_CHECKPOINT,
    CREATE_FILE,
    COMPLETE_FILE,
    SET_PINNED,
    RENAME,
    DELETE,
    CREATE_RAW_TABLE,
    UPDATE_RAW_TABLE_METADATA,
    CREATE_DEPENDENCY,
  }

  /**
   * Each entry in the EditLog is represented as a single Operation, which is serialized as JSON.
   * An Operation has a type, a transaction id, and a set of parameters determined by the type.
   */
  private static class Operation {
    public OperationType type;
    public long transId;
    public Map<String, Object> parameters = Maps.newHashMap();

    public Operation(OperationType type, long transId) {
      this.type = type;
      this.transId = transId;
    }

    /** Constructor used for deserializing Operations. */
    @JsonCreator
    public Operation(
        @JsonProperty("type") OperationType type,
        @JsonProperty("transId") long transId,
        @JsonProperty("parameters") Map<String, Object> parameters) {
      this.type = type;
      this.transId = transId;
      this.parameters = parameters;
    }

    /** Adds the given named parameter to the Operation. Value must be JSON-serializable. */
    public Operation withParameter(String name, Object value) {
      parameters.put(name, value);
      return this;
    }

    public Boolean getBoolean(String name) {
      return this.get(name);
    }

    public Integer getInt(String name) {
      return this.<Number>get(name).intValue();
    }

    /**
     * Deserializes the parameter as a long.
     * Use of this function is necessary when dealing with longs, as they may
     * have been deserialized as integers if they were sufficiently small.
     */
    public Long getLong(String name) {
      return this.<Number>get(name).longValue();
    }

    public String getString(String name) {
      return this.get(name);
    }

    /** Deserializes a base64-encoded String as a ByteBuffer. */
    public ByteBuffer getByteBuffer(String name) {
      String byteString = get(name);
      if (byteString == null) {
        return null;
      }

      return ByteBuffer.wrap(Base64.decodeBase64(byteString));
    }

    /** Deserializes a list of base64-encoded Strings as a list of ByteBuffers. */
    public List<ByteBuffer> getByteBufferList(String name) {
      List<String> byteStrings = get(name);
      if (byteStrings == null) {
        return null;
      }

      List<ByteBuffer> buffers = Lists.newArrayListWithCapacity(byteStrings.size());
      for (String byteString : byteStrings) {
        buffers.add(ByteBuffer.wrap(Base64.decodeBase64(byteString)));
      }
      return buffers;
    }

    /**
     * Generic parameter getter, useful for custom classes or enums.
     * Use a more specific getter, like getLong(), when available.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String name) {
      return (T) parameters.get(name);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("type", type)
          .add("transId", transId)
          .add("parameters", parameters)
          .toString();
    }
  }
}
