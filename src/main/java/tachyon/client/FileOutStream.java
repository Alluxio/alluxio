package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;

public class FileOutStream extends OutStream {

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final UserConf USER_CONF = UserConf.get();

  private final TachyonFS TFS;
  private final int FID;
  private final WriteType WRITE_TYPE;

  private long mSizeBytes;
  private ByteBuffer mBuffer;

  private RandomAccessFile mLocalFile;
  private FileChannel mLocalFileChannel;

  private OutputStream mCheckpointOutputStream;

  private boolean mClosed = false;
  private boolean mCancel = false;

  FileOutStream(TachyonFile file, WriteType opType) throws IOException {
    TFS = file.TFS;
    FID = file.FID;
    WRITE_TYPE = opType;

    mBuffer = ByteBuffer.allocate(USER_CONF.FILE_BUFFER_BYTES + 4);
    mBuffer.order(ByteOrder.nativeOrder());

    if (WRITE_TYPE.isCache()) {
      if (!TFS.hasLocalWorker()) {
        throw new IOException("No local worker on this machine.");
      }
      File localFolder = TFS.createAndGetUserTempFolder();
      if (localFolder == null) {
        throw new IOException("Failed to create temp user folder for tachyon client.");
      }
      String localFilePath = localFolder.getPath() + "/" + FID;
      mLocalFile = new RandomAccessFile(localFilePath, "rw");
      mLocalFileChannel = mLocalFile.getChannel();
      mSizeBytes = 0;
      LOG.info("File " + localFilePath + " was created!");
    }

    if (WRITE_TYPE.isThrough()) {
      String underfsFolder = TFS.createAndGetUserUnderfsTempFolder();
      UnderFileSystem underfsClient = UnderFileSystem.get(underfsFolder);
      mCheckpointOutputStream = underfsClient.create(underfsFolder + "/" + FID);
    }

  }

  @Override
  public void write(int b) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(byte[] b) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }
}
