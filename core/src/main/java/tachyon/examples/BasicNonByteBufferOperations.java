package tachyon.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import tachyon.Version;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

/**
 * Basic example of using the TachyonFS and TachyonFile for writing to and reading from files.
 * <p />
 * This class is different from {@link tachyon.examples.BasicOperations} in the way writes happen.
 * Over there {@link java.nio.ByteBuffer} is used directly, where as here byte data is done
 * via input/output streams.
 * <p />
 * This example also let users play around with how to work with files a bit more.  The
 * {@link tachyon.client.ReadType} is something that can be set, as well as ability to delete
 * file if exists.
 */
public final class BasicNonByteBufferOperations {
  private static void usage() {
    System.out.println("java -cp target/tachyon-" + Version.VERSION
        + "-jar-with-dependencies.jar "
        + BasicNonByteBufferOperations.class.getName()
        + " <master address> <file path> [write type] [read type] [delete file] [num writes]"
    );
    System.exit(-1);
  }

  public static void main(final String[] args) throws IOException {
    if (args.length < 2 || args.length > 6) {
      usage();
    }

    String address = args[0];
    String filePath = args[1];
    WriteType writeType = Utils.option(args, 2, WriteType.MUST_CACHE);
    ReadType readType = Utils.option(args, 3, ReadType.NO_CACHE);
    boolean deleteIfExists = Utils.option(args, 4, true);
    int length = Utils.option(args, 5, 20);

    TachyonFS client = TachyonFS.get(address);

    write(client, filePath, writeType, deleteIfExists, length);
    boolean passes = read(client, filePath, readType);

    Utils.printPassInfo(passes);
    System.exit(0);
  }

  private static void write(TachyonFS client, String filePath, WriteType writeType,
      boolean deleteIfExists, int length) throws IOException {
    // If the file exists already, we will override it.
    TachyonFile file = getOrCreate(client, filePath, deleteIfExists);
    DataOutputStream os = new DataOutputStream(file.getOutStream(writeType));
    try {
      os.writeInt(length);
      for (int i = 0; i < length; i ++) {
        os.writeInt(i);
      }
    } finally {
      os.close();
    }
  }

  private static TachyonFile getOrCreate(TachyonFS client, String filePath, boolean deleteIfExists)
      throws IOException {
    TachyonFile file = client.getFile(filePath);
    if (file == null) {
      // file doesn't exist yet, so create it
      int fileId = client.createFile(filePath);
      file = client.getFile(fileId);
    } else if (deleteIfExists) {
      // file exists, so delete it and recreate
      client.delete(file.getPath(), false);

      int fileId = client.createFile(filePath);
      file = client.getFile(fileId);
    }
    return file;
  }

  private static boolean read(TachyonFS client, String filePath, ReadType readType)
      throws IOException {
    TachyonFile file = client.getFile(filePath);
    DataInputStream input = new DataInputStream(file.getInStream(readType));
    boolean passes = true;
    try {
      int length = input.readInt();
      for (int i = 0; i < length; i ++) {
        passes &= (input.readInt() == i);
      }
    } finally {
      input.close();
    }
    return passes;
  }
}
