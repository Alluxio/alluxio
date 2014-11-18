package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.conf.UserConf;
import tachyon.util.CommonUtils;

public abstract class RemoteBlockReader {

  public static RemoteBlockReader createRemoteBlockReader() {
    try {
      Object readerObj =
          CommonUtils.createNewClassInstance(UserConf.get().REMOTE_BLOCK_READER_CLASS, null, null);
      Preconditions.checkArgument(readerObj instanceof RemoteBlockReader,
          "Remote Block Reader is not configured properly.");
      return (RemoteBlockReader) readerObj;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public abstract ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset,
      long length) throws IOException;
}
