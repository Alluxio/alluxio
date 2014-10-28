package tachyon.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.thrift.ClientFileInfo;

/**
 * Interface for Tachyon client APIs
 */
interface TachyonFSCore extends Closeable {
  /**
   * Creates a new file in the file system.
   * 
   * @param path The path of the file
   * @param ufsPath The path of the file in the under file system. If this is empty, the file does
   *        not exist in the under file system yet.
   * @param blockSizeByte The size of the block in bytes. It is -1 iff ufsPath is non-empty.
   * @param recursive Creates necessary parent folders if true, not otherwise.
   * @return The file id, which is globally unique.
   */
  int createFile(TachyonURI path, TachyonURI ufsPath, long blockSizeByte, boolean recursive)
      throws IOException;

  /**
   * Gets the ClientFileInfo object that represents the path.
   *
   * @param path the path of the file or folder.
   * @return the ClientFileInfo of the file or folder, null if the file or folder does not exist.
   * @throws IOException
   */
  ClientFileInfo getFileStatus(TachyonURI path) throws IOException;

  /** Returns a URI whose scheme and authority identify this FileSystem. */
  TachyonURI getUri();

  /**
   * If the <code>path</code> is a directory, return all the direct entries in it. If the
   * <code>path</code> is a file, return its ClientFileInfo.
   * 
   * @param path the target directory/file path
   * @return A list of ClientFileInfo, null if the file or folder does not exist.
   * @throws IOException
   */
  List<ClientFileInfo> listStatus(TachyonURI path) throws IOException;

  /**
   * Creates a folder.
   * 
   * @param path the path of the folder to be created
   * @param recursive Creates necessary parent folders if true, not otherwise.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   */
  boolean mkdirs(TachyonURI path, boolean recursive) throws IOException;
}
