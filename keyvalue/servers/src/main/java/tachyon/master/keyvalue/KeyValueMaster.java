/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.keyvalue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.exception.AccessControlException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.master.MasterBase;
import tachyon.master.MasterContext;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.file.options.CreateDirectoryOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.journal.JournalProtoUtils;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.proto.journal.KeyValue.CompletePartitionEntry;
import tachyon.proto.journal.KeyValue.CompleteStoreEntry;
import tachyon.proto.journal.KeyValue.CreateStoreEntry;
import tachyon.proto.journal.KeyValue.DeleteStoreEntry;
import tachyon.proto.journal.KeyValue.MergeStoreEntry;
import tachyon.thrift.KeyValueMasterClientService;
import tachyon.thrift.PartitionInfo;
import tachyon.util.IdUtils;
import tachyon.util.io.PathUtils;

/**
 * The key-value master stores key-value store information in Tachyon, including the partitions of
 * each key-value store.
 */
@ThreadSafe
public final class KeyValueMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final FileSystemMaster mFileSystemMaster;

  /** Map from file id of a complete store to the list of partitions in this store. */
  private final Map<Long, List<PartitionInfo>> mCompleteStoreToPartitions;
  /**
   * Map from file id of an incomplete store (i.e., some one is still writing new partitions) to the
   * list of partitions in this store.
   */
  private final Map<Long, List<PartitionInfo>> mIncompleteStoreToPartitions;

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.KEY_VALUE_MASTER_NAME);
  }

  /**
   * @param fileSystemMaster handler to a {@link FileSystemMaster} to use for filesystem operations
   * @param journal a {@link Journal} to write journal entries to
   */
  public KeyValueMaster(FileSystemMaster fileSystemMaster, Journal journal) {
    super(journal, 2);
    mFileSystemMaster = fileSystemMaster;
    mCompleteStoreToPartitions = Maps.newHashMap();
    mIncompleteStoreToPartitions = Maps.newHashMap();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_NAME,
        new KeyValueMasterClientService.Processor<KeyValueMasterClientServiceHandler>(
            new KeyValueMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.KEY_VALUE_MASTER_NAME;
  }

  @Override
  public synchronized void processJournalEntry(JournalEntry entry) throws IOException {
    Message innerEntry = JournalProtoUtils.unwrap(entry);
    try {
      if (innerEntry instanceof CreateStoreEntry) {
        createStoreFromEntry((CreateStoreEntry) innerEntry);
      } else if (innerEntry instanceof CompletePartitionEntry) {
        completePartitionFromEntry((CompletePartitionEntry) innerEntry);
      } else if (innerEntry instanceof CompleteStoreEntry) {
        completeStoreFromEntry((CompleteStoreEntry) innerEntry);
      } else if (innerEntry instanceof DeleteStoreEntry) {
        deleteStoreFromEntry((DeleteStoreEntry) innerEntry);
      } else if (innerEntry instanceof MergeStoreEntry) {
        mergeStoreFromEntry((MergeStoreEntry) innerEntry);
      } else {
        throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(innerEntry));
      }
    } catch (TachyonException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void streamToJournalCheckpoint(JournalOutputStream outputStream)
      throws IOException {
    for (Map.Entry<Long, List<PartitionInfo>> entry : mCompleteStoreToPartitions.entrySet()) {
      long fileId = entry.getKey();
      List<PartitionInfo> partitions = entry.getValue();
      outputStream.writeEntry(newCreateStoreEntry(fileId));
      for (PartitionInfo info : partitions) {
        outputStream.writeEntry(newCompletePartitionEntry(fileId, info));
      }
      outputStream.writeEntry(newCompleteStoreEntry(fileId));
    }
    for (Map.Entry<Long, List<PartitionInfo>> entry : mIncompleteStoreToPartitions.entrySet()) {
      long fileId = entry.getKey();
      List<PartitionInfo> partitions = entry.getValue();
      outputStream.writeEntry(newCreateStoreEntry(fileId));
      for (PartitionInfo info : partitions) {
        outputStream.writeEntry(newCompletePartitionEntry(fileId, info));
      }
    }
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
  }

  /**
   * Marks a partition complete and adds it to an incomplete key-value store.
   *
   * @param path URI of the key-value store
   * @param info information of this completed parition
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   * @throws AccessControlException if permission checking fails
   */
  public synchronized void completePartition(TachyonURI path, PartitionInfo info)
      throws FileDoesNotExistException, AccessControlException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      throw new FileDoesNotExistException(
          String.format("Failed to completePartition: path %s does not exist", path));
    }

    completePartitionInternal(fileId, info);

    writeJournalEntry(newCompletePartitionEntry(fileId, info));
    flushJournal();
  }

  // Marks a partition complete, called when replaying journals
  private void completePartitionFromEntry(CompletePartitionEntry entry)
      throws FileDoesNotExistException {
    PartitionInfo info = new PartitionInfo(entry.getKeyStartBytes().asReadOnlyByteBuffer(),
        entry.getKeyLimitBytes().asReadOnlyByteBuffer(), entry.getBlockId());
    completePartitionInternal(entry.getStoreId(), info);
  }

  // Internal implementation to mark a partition complete
  private void completePartitionInternal(long fileId, PartitionInfo info)
      throws FileDoesNotExistException {
    if (!mIncompleteStoreToPartitions.containsKey(fileId)) {
      // TODO(binfan): throw a better exception
      throw new FileDoesNotExistException(String.format(
          "Failed to completeStore: KeyValueStore (fileId=%d) was not created before", fileId));
    }
    // NOTE: deep copy the partition info object
    mIncompleteStoreToPartitions.get(fileId).add(new PartitionInfo(info));
  }

  /**
   * Marks a key-value store complete.
   *
   * @param path URI of the key-value store
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   * @throws AccessControlException if permission checking fails
   */
  public synchronized void completeStore(TachyonURI path) throws FileDoesNotExistException,
      AccessControlException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      throw new FileDoesNotExistException(
          String.format("Failed to completeStore: path %s does not exist", path));
    }
    completeStoreInternal(fileId);
    writeJournalEntry(newCompleteStoreEntry(fileId));
    flushJournal();
  }

  // Marks a store complete, called when replaying journals
  private void completeStoreFromEntry(CompleteStoreEntry entry) throws FileDoesNotExistException {
    completeStoreInternal(entry.getStoreId());
  }

  // Internal implementation to mark a store complete
  private void completeStoreInternal(long fileId) throws FileDoesNotExistException {
    if (!mIncompleteStoreToPartitions.containsKey(fileId)) {
      // TODO(binfan): throw a better exception
      throw new FileDoesNotExistException(String.format(
          "Failed to completeStore: KeyValueStore (fileId=%d) was not created before", fileId));
    }
    List<PartitionInfo> partitions = mIncompleteStoreToPartitions.remove(fileId);
    mCompleteStoreToPartitions.put(fileId, partitions);
  }

  /**
   * Creates a new key-value store.
   *
   * @param path URI of the key-value store
   * @throws FileAlreadyExistsException if a key-value store URI exists
   * @throws AccessControlException if permission checking fails
   */
  public synchronized void createStore(TachyonURI path)
      throws FileAlreadyExistsException, InvalidPathException, AccessControlException {
    try {
      // Create this dir
      mFileSystemMaster.mkdir(path,
          new CreateDirectoryOptions.Builder(MasterContext.getConf()).setRecursive(true).build());
    } catch (IOException e) {
      // TODO(binfan): Investigate why mFileSystemMaster.mkdir throws IOException
      throw new InvalidPathException(
          String.format("Failed to createStore: can not create path %s", path), e);
    }
    final long fileId = mFileSystemMaster.getFileId(path);
    Preconditions.checkState(fileId != IdUtils.INVALID_FILE_ID);

    createStoreInternal(fileId);
    writeJournalEntry(newCreateStoreEntry(fileId));
    flushJournal();
  }

  // Creates a store, called when replaying journals
  private void createStoreFromEntry(CreateStoreEntry entry) throws FileAlreadyExistsException {
    createStoreInternal(entry.getStoreId());
  }

  // Internal implementation to create a store
  private void createStoreInternal(long fileId) throws FileAlreadyExistsException {
    if (mIncompleteStoreToPartitions.containsKey(fileId)) {
      // TODO(binfan): throw a better exception
      throw new FileAlreadyExistsException(String
          .format("Failed to createStore: KeyValueStore (fileId=%d) is already created", fileId));
    }
    mIncompleteStoreToPartitions.put(fileId, Lists.<PartitionInfo>newArrayList());
  }

  /**
   * Deletes a completed key-value store.
   *
   * @param uri {@link TachyonURI} to the store
   * @throws IOException if non-Tachyon error occurs
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   * @throws TachyonException if other Tachyon error occurs
   */
  public synchronized void deleteStore(TachyonURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, TachyonException {
    long fileId = getFileId(uri);
    checkIsCompletePartition(fileId, uri);
    mFileSystemMaster.deleteFile(uri, true);
    deleteStoreInternal(fileId);
    writeJournalEntry(newDeleteStoreEntry(fileId));
    flushJournal();
  }

  // Deletes a store, called when replaying journals.
  private void deleteStoreFromEntry(DeleteStoreEntry entry) {
    deleteStoreInternal(entry.getStoreId());
  }

  // Internal implementation to deleteStore a key-value store.
  private void deleteStoreInternal(long fileId) {
    mCompleteStoreToPartitions.remove(fileId);
  }

  private long getFileId(TachyonURI uri) throws AccessControlException, FileDoesNotExistException {
    long fileId = mFileSystemMaster.getFileId(uri);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(uri));
    }
    return fileId;
  }

  void checkIsCompletePartition(long fileId, TachyonURI uri) throws InvalidPathException {
    if (!mCompleteStoreToPartitions.containsKey(fileId)) {
      throw new InvalidPathException(ExceptionMessage.INVALID_KEY_VALUE_STORE_URI.getMessage(uri));
    }
  }

  /**
   * Merges one completed key-value store to another completed key-value store.
   *
   * @param fromUri the {@link TachyonURI} to the store to be merged
   * @param toUri the {@link TachyonURI} to the store to be merged to
   * @throws IOException if non-Tachyon error occurs
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   * @throws TachyonException if other Tachyon error occurs
   */
  public synchronized void mergeStore(TachyonURI fromUri, TachyonURI toUri)
      throws IOException, FileDoesNotExistException, InvalidPathException, TachyonException {
    long fromFileId = getFileId(fromUri);
    long toFileId = getFileId(toUri);
    checkIsCompletePartition(fromFileId, fromUri);
    checkIsCompletePartition(toFileId, toUri);

    // Rename fromUri to "toUri/%s-%s" % (last component of fromUri, UUID).
    // NOTE: rename does not change the existing block IDs.
    mFileSystemMaster.rename(fromUri, new TachyonURI(PathUtils.concatPath(toUri.toString(),
        String.format("%s-%s", fromUri.getName(), UUID.randomUUID().toString()))));
    mergeStoreInternal(fromFileId, toFileId);

    writeJournalEntry(newMergeStoreEntry(fromFileId, toFileId));
    flushJournal();
  }

  // Internal implementation to merge two completed stores.
  private void mergeStoreInternal(long fromFileId, long toFileId) {
    // Move partition infos to the new store.
    List<PartitionInfo> partitionsToBeMerged = mCompleteStoreToPartitions.remove(fromFileId);
    mCompleteStoreToPartitions.get(toFileId).addAll(partitionsToBeMerged);
  }

  // Merges two completed stores, called when replaying journals.
  private void mergeStoreFromEntry(MergeStoreEntry entry) {
    mergeStoreInternal(entry.getFromStoreId(), entry.getToStoreId());
  }

  /**
   * Gets a list of partitions of a given key-value store.
   *
   * @param path URI of the key-value store
   * @return a list of partition information
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   * @throws AccessControlException if permission checking fails
   */
  public synchronized List<PartitionInfo> getPartitionInfo(TachyonURI path)
      throws FileDoesNotExistException, AccessControlException {
    long fileId = getFileId(path);
    List<PartitionInfo> partitions = mCompleteStoreToPartitions.get(fileId);
    if (partitions == null) {
      return Lists.newArrayList();
    }
    return partitions;
  }

  private JournalEntry newCreateStoreEntry(long fileId) {
    CreateStoreEntry createStore = CreateStoreEntry.newBuilder().setStoreId(fileId).build();
    return JournalEntry.newBuilder().setCreateStore(createStore).build();
  }

  private JournalEntry newCompletePartitionEntry(long fileId, PartitionInfo info) {
    CompletePartitionEntry completePartition =
        CompletePartitionEntry.newBuilder().setStoreId(fileId).setBlockId(info.blockId)
            .setKeyStartBytes(ByteString.copyFrom(info.keyStart))
            .setKeyLimitBytes(ByteString.copyFrom(info.keyLimit)).build();
    return JournalEntry.newBuilder().setCompletePartition(completePartition).build();
  }

  private JournalEntry newCompleteStoreEntry(long fileId) {
    CompleteStoreEntry completeStore = CompleteStoreEntry.newBuilder().setStoreId(fileId).build();
    return JournalEntry.newBuilder().setCompleteStore(completeStore).build();
  }

  private JournalEntry newDeleteStoreEntry(long fileId) {
    DeleteStoreEntry deleteStore = DeleteStoreEntry.newBuilder().setStoreId(fileId).build();
    return JournalEntry.newBuilder().setDeleteStore(deleteStore).build();
  }

  private JournalEntry newMergeStoreEntry(long fromFileId, long toFileId) {
    MergeStoreEntry mergeStore = MergeStoreEntry.newBuilder().setFromStoreId(fromFileId)
        .setToStoreId(toFileId).build();
    return JournalEntry.newBuilder().setMergeStore(mergeStore).build();
  }
}
