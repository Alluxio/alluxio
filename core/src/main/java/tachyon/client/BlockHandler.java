package tachyon.client;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.TachyonURI;

/**
 * Base class for handling block files. Block handlers for different under file systems can be
 * implemented by extending this class. It is not thread safe, the caller must guarantee thread
 * safe. This class is internal and subject to changes.
 */
abstract class BlockHandler implements Closeable {

  /**
   * Create a block handler according to path scheme
   * 
   * @param path
   *          block file path
   * @return block handler of the block file
   * @throws IOException
   */
  public static BlockHandler get(String path) throws IOException, IllegalArgumentException {
    if (path.startsWith(TachyonURI.SEPARATOR) || path.startsWith("file://")) {
      return new BlockHandlerLocal(path);
    }
    throw new IllegalArgumentException("Unsupported block file path: " + path);
  }

  /**
   * Append data to block file from byte array
   * 
   * @param blockOffset
   *          starting position of the block file
   * @param buf
   *          buffer that data is stored in
   * @param offset
   *          offset of the buf
   * @param length
   *          length of the data
   * @return size of data that is written
   * @throws IOException
   */
  public int append(long blockOffset, byte[] buf, int offset, int length) throws IOException {
    return append(blockOffset, ByteBuffer.wrap(buf, offset, length));
  }

  /**
   * Append data to block file from ByteBuffer
   * 
   * @param blockOffset
   *          starting position of the block file
   * @param srcBuf
   *          ByteBuffer that data is stored in
   * @return size of data that is written
   * @throws IOException
   */
  public abstract int append(long blockOffset, ByteBuffer srcBuf) throws IOException;

  /**
   * Delete block file
   * 
   * @return true if success, otherwise false
   * @throws IOException
   */
  public abstract boolean delete() throws IOException;

  /**
   * Read data from block file
   * 
   * @param blockOffset
   *          offset from starting of the block file
   * @param length
   *          length of data to read, -1 represents reading the rest of the block file
   * @return ByteBuffer storing data that is read
   * @throws IOException
   */
  public abstract ByteBuffer read(long blockOffset, int length) throws IOException;
}
