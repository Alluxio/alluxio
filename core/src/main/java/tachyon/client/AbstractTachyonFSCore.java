package tachyon.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import tachyon.thrift.ClientFileInfo;

abstract class AbstractTachyonFSCore implements Closeable {

  // TODO add this when TachyonURI works.
  // /** Returns a URI whose scheme and authority identify this FileSystem. */
  // public abstract TachyonURI getUri();

  /**
   * Creates a new file in the file system.
   * 
   * @param path
   *          The path of the file
   * @param ufsPath
   *          The path of the file in the under file system. If this is empty or null, the file
   *          does not exist in the under file system yet.
   * @param blockSizeByte
   *          The size of the block in bytes. It is -1 iff ufsPath is non-empty.
   * @param recursive
   *          Creates necessary parent folders if true, not otherwise.
   * @return The file id, which is globally unique.
   */
  public abstract int
  createFile(String path, String ufsPath, long blockSizeByte, boolean recursive)
      throws IOException;

  /**
   * Deletes a file or folder
   * 
   * @param fileId
   *          The id of the file / folder. If it is not -1, path parameter is ignored. Otherwise,
   *          the method uses the path parameter.
   * @param path
   *          The path of the file / folder. It could be empty or null iff id is not -1.
   * @param recursive
   *          If fileId or path represents a non-empty folder, delete the folder recursively or not
   * @return true if deletes successfully, false otherwise.
   * @throws IOException
   */
  public abstract boolean delete(int fileId, String path, boolean recursive) throws IOException;

  /**
   * Return a ClientFileInfo object that represents the fid, or the path if fid is -1.
   * 
   * @param fid
   *          the file id of the file or folder.
   * @param path
   *          the path of the file or folder. valid iff fid is -1.
   * @return the ClientFileInfo of the file or folder, null if the file or folder does not exist.
   * @throws IOException
   */
  public abstract ClientFileInfo getFileStatus(int fid, String path) throws IOException;

  /**
   * If the <code>path</code> is a directory, return all the direct entries in it. If the
   * <code>path</code> is a file, return its ClientFileInfo.
   * 
   * @param path
   *          the target directory/file path
   * @return A list of ClientFileInfo, null if the file or folder does not exist.
   * @throws IOException
   */
  public abstract List<ClientFileInfo> listStatus(String path) throws IOException;

  /**
   * Creates a folder.
   * 
   * @param path
   *          the path of the folder to be created
   * @param recursive
   *          Creates necessary parent folders if true, not otherwise.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   */
  public abstract boolean mkdirs(String path, boolean recursive) throws IOException;

  /**
   * Renames a file or folder to another path.
   * 
   * @param fileId
   *          The id of the source file / folder. If it is not -1, path parameter is ignored.
   *          Otherwise, the method uses the srcPath parameter.
   * @param srcPath
   *          The path of the source file / folder. It could be empty or null iff id is not -1.
   * @param dstPath
   *          The path of the destination file / folder. It could be empty or null iff id is not -1.
   * @return true if renames successfully, false otherwise.
   * @throws IOException
   */
  public abstract boolean rename(int fileId, String srcPath, String dstPath) throws IOException;
}
