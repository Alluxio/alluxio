package tachyon;

import java.io.IOException;

/**
 * Utility functions for working with {@link tachyon.UnderFileSystem}.
 * 
 * TODO May move this to non-testing code.
 */
public final class UnderFileSystemsUtils {
  private UnderFileSystemsUtils() {}

  /**
   * Deletes the directory at the given path. If delete is unsuccessful, then this operation will
   * throw a {@link java.io.IOException}.
   */
  public static void deleteDir(final String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  /**
   * Attempts to create the directory if it does not already exist. If unable to create the
   * directory, then a {@link java.io.IOException} is thrown.
   */
  public static void mkdirIfNotExists(final String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (!ufs.exists(path)) {
      if (!ufs.mkdirs(path, true)) {
        throw new IOException("Failed to make folder: " + path);
      }
    }
  }
}
