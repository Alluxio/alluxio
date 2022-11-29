package alluxio.master.metastore.kvstore;

import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.proto.kvstore.FileCacheStatus;
import alluxio.proto.kvstore.FileEntryKey;
import alluxio.proto.kvstore.FileEntryValue;
import alluxio.proto.meta.InodeMeta;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class KVStoreUtils {
  public static MutableInode<?> convertToMutableInode(FileEntryKey key, FileEntryValue value,
      FileCacheStatus cacheStatus) {
    switch (value.getMEntryType()) {
      case FILE:
        InodeMeta.Inode.newBuilder();
        CreateFileContext createFileContext = CreateFileContext.defaults();
        // createFileContext.getOptions().setMode();
        return MutableInodeFile.create(key.getParentID(), value.getMID(),
            key.getName(), System.currentTimeMillis(), createFileContext);
      case DIRECTORY:
        CreateDirectoryContext createDirectoryContext = CreateDirectoryContext.defaults();
        // createDirectoryContext.getOptions().set
        return MutableInodeDirectory.create(key.getParentID(), value.getMID(),
            key.getName(), createDirectoryContext);
      default:
        return null;
    }
  }

  public static ByteString convertMutableInodeToByteString(MutableInode<?> inode) {
    return inode.toProto().toByteString();
  }

  public static MutableInode<?> convertToMutableInode(FileEntryKey key, FileEntryValue value) {
    try {
      MutableInode<?> inode = MutableInode.fromProto(InodeMeta.Inode.parseFrom(value.getMEV()));
      return inode;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static InodeMeta.InodeCacheAttri convertToInodeCacheAttr(FileCacheStatus value) {
    try {
      return InodeMeta.InodeCacheAttri.parseFrom(value.getMCacheValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static ByteString convertInodeCacheAttrToByteString(
      InodeMeta.InodeCacheAttri inodeCacheAttri) {
    return inodeCacheAttri.toByteString();
  }
}
