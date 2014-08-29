package tachyon.client;

import java.io.IOException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.UserConf;

/**
 * Abstract implementation of {@code TachyonFSCore} APIs.
 */
abstract class AbstractTachyonFS implements TachyonFSCore {
  /**
   * Creates a file with the default block size (1GB) in the system. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   * @deprecated use {@link #createFile(TachyonURI)} instead
   */
  @Deprecated
  public synchronized int createFile(String path) throws IOException {
    return createFile(new TachyonURI(path));
  }

  /**
   * Creates a file with the default block size (1GB) in the system. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   */
  public synchronized int createFile(TachyonURI path) throws IOException {
    return createFile(path, UserConf.get().DEFAULT_BLOCK_SIZE_BYTE);
  }

  /**
   * Creates a file in the system. It also creates necessary folders along the path.
   * // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @param blockSizeByte
   *          the block size of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   * @deprecated use {@link #createFile(TachyonURI, long)} instead
   */
  @Deprecated
  public synchronized int createFile(String path, long blockSizeByte) throws IOException {
    if (blockSizeByte > (long) Constants.GB * 2) {
      throw new IOException("Block size must be less than 2GB: " + blockSizeByte);
    }

    return createFile(new TachyonURI(path), blockSizeByte);
  }

  /**
   * Creates a file in the system. It also creates necessary folders along the path.
   * // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @param blockSizeByte
   *          the block size of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   */
  public synchronized int createFile(TachyonURI path, long blockSizeByte) throws IOException {
    if (blockSizeByte > (long) Constants.GB * 2) {
      throw new IOException("Block size must be less than 2GB: " + blockSizeByte);
    }

    return createFile(path, new TachyonURI(""), blockSizeByte, true);
  }

  /**
   * Creates a file in the system with a pre-defined underfsPath. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file in Tachyon
   * @param ufsPath
   *          the path of the file in the underfs
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   * @deprecated use {@link #createFile(TachyonURI, TachyonURI)} instead
   */
  @Deprecated
  public synchronized int createFile(String path, String ufsPath) throws IOException {
    return createFile(new TachyonURI(path), new TachyonURI(ufsPath));
  }

  /**
   * Creates a file in the system with a pre-defined underfsPath. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file in Tachyon
   * @param ufsPath
   *          the path of the file in the underfs
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   */
  public synchronized int createFile(TachyonURI path, TachyonURI ufsPath) throws IOException {
    return createFile(path, ufsPath, -1, true);
  }

  /**
   * Deletes the file denoted by the file id.
   * 
   * @param fid
   *          file id
   * @param recursive
   *          if delete the path recursively.
   * @return true if deletion succeed (including the case the file does not exist in the first
   *         place), false otherwise.
   * @throws IOException
   */
  public synchronized boolean delete(int fid, boolean recursive) throws IOException {
    return delete(fid, new TachyonURI(""), recursive);
  }

  /**
   * Deletes the file denoted by the path.
   *
   * @param path
   *          the file path
   * @param recursive
   *          if delete the path recursively.
   * @return true if the deletion succeed (including the case that the path does not exist in the
   *         first place), false otherwise.
   * @throws IOException
   * @deprecated use {@link #delete(TachyonURI, boolean)} instead
   */
  @Deprecated
  public synchronized boolean delete(String path, boolean recursive) throws IOException {
    return delete(new TachyonURI(path), recursive);
  }

  /**
   * Deletes the file denoted by the path.
   *
   * @param path
   *          the file path
   * @param recursive
   *          if delete the path recursively.
   * @return true if the deletion succeed (including the case that the path does not exist in the
   *         first place), false otherwise.
   * @throws IOException
   */
  public synchronized boolean delete(TachyonURI path, boolean recursive) throws IOException {
    return delete(-1, path, recursive);
  }

  /**
   * Create a directory if it does not exist. The method also creates necessary non-existing
   * parent folders.
   *
   * @param path
   *          Directory path.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   * @deprecated use {@link #mkdir(TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean mkdir(String path) throws IOException {
    return mkdir(new TachyonURI(path));
  }

  /**
   * Create a directory if it does not exist. The method also creates necessary non-existing
   * parent folders.
   *
   * @param path
   *          Directory path.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   */
  public synchronized boolean mkdir(TachyonURI path) throws IOException {
    return mkdirs(path, true);
  }

  /**
   * Renames the file
   * 
   * @param fileId
   *          the file id
   * @param dstPath
   *          the new path of the file in the file system.
   * @return true if succeed, false otherwise
   * @throws IOException
   * @deprecated use {@link #rename(int, TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean rename(int fileId, String dstPath) throws IOException {
    return rename(fileId, new TachyonURI(dstPath));
  }

  /**
   * Renames the file
   *
   * @param fileId
   *          the file id
   * @param dstPath
   *          the new path of the file in the file system.
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean rename(int fileId, TachyonURI dstPath) throws IOException {
    return rename(fileId, new TachyonURI(""), dstPath);
  }

  /**
   * Rename the srcPath to the dstPath
   *
   * @param srcPath
   *          The path of the source file / folder.
   * @param dstPath
   *          The path of the destination file / folder.
   * @return true if succeed, false otherwise.
   * @throws IOException
   * @deprecated use {@link #rename(TachyonURI, TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean rename(String srcPath, String dstPath) throws IOException {
    return rename(new TachyonURI(srcPath), new TachyonURI(dstPath));
  }

  /**
   * Rename the srcPath to the dstPath
   *
   * @param srcPath
   *          The path of the source file / folder.
   * @param dstPath
   *          The path of the destination file / folder.
   * @return true if succeed, false otherwise.
   * @throws IOException
   */
  public synchronized boolean rename(TachyonURI srcPath, TachyonURI dstPath) throws IOException {
    return rename(-1, srcPath, dstPath);
  }
}
