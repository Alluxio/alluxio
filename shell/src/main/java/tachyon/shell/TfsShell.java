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

package tachyon.shell;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.lineage.TachyonLineage;
import tachyon.client.lineage.options.DeleteLineageOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.FileInfo;
import tachyon.thrift.LineageInfo;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

/**
 * Class for handling command line inputs.
 */
public class TfsShell implements Closeable {
  /**
   * Main method, starts a new TfsShell
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    TfsShell shell = new TfsShell(new TachyonConf());
    int ret;
    try {
      ret = shell.run(argv);
    } finally {
      shell.close();
    }
    System.exit(ret);
  }

  private final Closer mCloser;
  private final TachyonConf mTachyonConf;
  private final TachyonFileSystem mTfs;

  public TfsShell(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mCloser = Closer.create();
    mTfs = TachyonFileSystemFactory.get();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * Prints the file's contents to the console.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void cat(TachyonURI path) throws IOException {
    try {
      TachyonFile fd = mTfs.open(path);
      FileInfo tFile = mTfs.getInfo(fd);

      if (!tFile.isFolder) {
        InStreamOptions op = new InStreamOptions.Builder(mTachyonConf)
            .setTachyonStorageType(TachyonStorageType.NO_STORE).build();
        FileInStream is = mTfs.getInStream(fd, op);
        byte[] buf = new byte[512];
        try {
          int read = is.read(buf);
          while (read != -1) {
            System.out.write(buf, 0, read);
            read = is.read(buf);
          }
        } finally {
          is.close();
        }
      } else {
        throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Load a file or directory in Tachyon space, makes it resident in memory.
   *
   * @param filePath The TachyonURI path to load into Tachyon memory
   * @throws IOException
   */
  public void load(TachyonURI filePath) throws IOException {
    try {
      TachyonFile fd = mTfs.open(filePath);
      FileInfo fInfo = mTfs.getInfo(fd);
      if (fInfo.isFolder) {
        List<FileInfo> files = mTfs.listStatus(fd);
        Collections.sort(files);
        for (FileInfo file : files) {
          TachyonURI newPath = new TachyonURI(file.getPath());
          load(newPath);
        }
      } else {
        Closer closer = Closer.create();
        try {
          InStreamOptions op = new InStreamOptions.Builder(mTachyonConf)
              .setTachyonStorageType(TachyonStorageType.STORE).build();
          FileInStream in = closer.register(mTfs.getInStream(fd, op));
          byte[] buf = new byte[8 * Constants.MB];
          while (in.read(buf) != -1) {
          }
        } catch (Throwable e) {
          throw closer.rethrow(e);
        } finally {
          closer.close();
        }
      }
      System.out.println(filePath + " loaded");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Copies a list of files or directories specified by srcFiles from the local filesystem to
   * dstPath in the Tachyon filesystem space. This method is used when the input path contains
   * wildcards.
   *
   * @param srcFiles The list of files in the local filesystem
   * @param dstPath The TachyonURI of the destination
   * @throws IOException
   */
  public void copyFromLocalWildcard(List<File> srcFiles, TachyonURI dstPath) throws IOException {
    try {
      mTfs.mkdir(dstPath);
    } catch (FileAlreadyExistsException e) {
      // it's fine if the directory already exists
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    FileInfo dstFileInfo;
    try {
      TachyonFile dstFd = mTfs.open(dstPath);
      dstFileInfo = mTfs.getInfo(dstFd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    if (!dstFileInfo.isFolder) {
      throw new IOException(
          ExceptionMessage.DESTINATION_FILE_CANNOT_EXIST_WITH_WILDCARD_SOURCE.getMessage());
    }

    List<String> errorMessages = new ArrayList<String>();
    for (File srcFile : srcFiles) {
      try {
        copyFromLocal(srcFile,
            new TachyonURI(PathUtils.concatPath(dstPath.getPath(), srcFile.getName())));
      } catch (IOException e) {
        errorMessages.add(e.getMessage());
      }
    }
    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a file or directory specified by srcPath from the local filesystem to dstPath in the
   * Tachyon filesystem space. Will fail if the path given already exists in the filesystem.
   *
   * @param srcFile The source file in the local filesystem
   * @param dstPath The TachyonURI of the destination
   * @throws IOException
   */
  public void copyFromLocal(File srcFile, TachyonURI dstPath) throws IOException {
    copyPath(srcFile, mTfs, dstPath);
    System.out.println("Copied " + srcFile.getPath() + " to " + dstPath);
  }

  private void copyPath(File src, TachyonFileSystem tachyonClient, TachyonURI dstPath)
      throws IOException {
    try {
      if (!src.isDirectory()) {
        // If the dstPath is a directory, then it should be updated to be the path of the file where
        // src will be copied to
        TachyonFile fd = tachyonClient.openIfExists(dstPath);
        if (fd != null) {
          FileInfo tFile = tachyonClient.getInfo(fd);
          if (tFile.isFolder) {
            dstPath = dstPath.join(src.getName());
          }
        }

        Closer closer = Closer.create();
        FileOutStream os = null;
        try {
          os = closer.register(tachyonClient.getOutStream(dstPath, OutStreamOptions.defaults()));
          FileInputStream in = closer.register(new FileInputStream(src));
          FileChannel channel = closer.register(in.getChannel());
          ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
          while (channel.read(buf) != -1) {
            buf.flip();
            os.write(buf.array(), 0, buf.limit());
          }
        } catch (IOException e) {
          // Close the out stream and delete the file, so we don't have an incomplete file lying
          // around
          if (os != null) {
            os.cancel();
            fd = tachyonClient.openIfExists(dstPath);
            if (fd != null) {
              tachyonClient.delete(fd);
            }
          }
          throw e;
        } finally {
          closer.close();
        }
      } else {
        tachyonClient.mkdir(dstPath);
        List<String> errorMessages = new ArrayList<String>();
        String[] fileList = src.list();
        for (String file : fileList) {
          TachyonURI newPath = new TachyonURI(dstPath, new TachyonURI(file));
          File srcFile = new File(src, file);
          try {
            copyPath(srcFile, tachyonClient, newPath);
          } catch (IOException e) {
            errorMessages.add(e.getMessage());
          }
        }
        if (errorMessages.size() != 0) {
          if (errorMessages.size() == fileList.length) {
            // If no files were created, then delete the directory
            TachyonFile f = tachyonClient.openIfExists(dstPath);
            if (f != null) {
              tachyonClient.delete(f);
            }
          }
          throw new IOException(Joiner.on('\n').join(errorMessages));
        }
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Copies a list of files or directories specified by srcPaths from the Tachyon filesystem to
   * dstPath in the local filesystem. This method is used when the input path contains wildcards.
   *
   * @param srcPaths The list of files in the Tachyon filesystem
   * @param dstFile The destination directory in the local filesystem
   * @throws IOException
   */
  public void copyWildcardToLocal(List<TachyonURI> srcPaths, File dstFile) throws IOException {
    if (dstFile.exists() && !dstFile.isDirectory()) {
      throw new IOException(
          ExceptionMessage.DESTINATION_FILE_CANNOT_EXIST_WITH_WILDCARD_SOURCE.getMessage());
    }
    if (!dstFile.exists()) {
      if (!dstFile.mkdirs()) {
        throw new IOException("Fail to create directory: " + dstFile.getPath());
      } else {
        System.out.println("Create directory: " + dstFile.getPath());
      }
    }
    List<String> errorMessages = new ArrayList<String>();
    for (TachyonURI srcPath : srcPaths) {
      try {
        copyToLocal(srcPath, new File(dstFile.getAbsoluteFile(), srcPath.getName()));
      } catch (IOException e) {
        errorMessages.add(e.getMessage());
      }
    }
    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a file or a directory from the Tachyon filesystem to the local filesystem.
   *
   * @param srcPath The source TachyonURI (could be a file or a directory)
   * @param dstFile The destination file in the local filesystem
   * @throws IOException
   */
  private void copyToLocal(TachyonURI srcPath, File dstFile) throws IOException {
    TachyonFile srcFd;
    FileInfo srcFileInfo;
    try {
      srcFd = mTfs.open(srcPath);
      srcFileInfo = mTfs.getInfo(srcFd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (srcFileInfo.isFolder) {
      // make a local directory
      if (!dstFile.exists()) {
        if (!dstFile.mkdirs()) {
          throw new IOException("mkdir failure for directory: " + dstFile.getAbsolutePath());
        } else {
          System.out.println("Create directory: " + dstFile.getAbsolutePath());
        }
      }

      List<FileInfo> files = null;
      try {
        files = mTfs.listStatus(srcFd);
      } catch (TachyonException e) {
        throw new IOException(e.getMessage());
      }

      List<String> errorMessages = new ArrayList<String>();
      for (FileInfo file : files) {
        try {
          copyToLocal(new TachyonURI(srcPath.getScheme(), srcPath.getAuthority(), file.getPath()),
              new File(dstFile.getAbsolutePath(), file.getName()));
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } else {
      copyFileToLocal(srcPath, dstFile);
    }
  }

  /**
   * Copies a file specified by argv from the filesystem to the local filesystem. This is the
   * utility function.
   *
   * @param srcPath The source TachyonURI (has to be a file)
   * @param dstFile The destination file in the local filesystem
   * @throws IOException
   */
  public void copyFileToLocal(TachyonURI srcPath, File dstFile) throws IOException {
    try {
      TachyonFile srcFd = mTfs.open(srcPath);
      File tmpDst = File.createTempFile("copyToLocal", null);
      tmpDst.deleteOnExit();

      Closer closer = Closer.create();
      try {
        InStreamOptions op = new InStreamOptions.Builder(mTachyonConf)
            .setTachyonStorageType(TachyonStorageType.NO_STORE).build();
        FileInStream is = closer.register(mTfs.getInStream(srcFd, op));
        FileOutputStream out = closer.register(new FileOutputStream(tmpDst));
        byte[] buf = new byte[64 * Constants.MB];
        int t = is.read(buf);
        while (t != -1) {
          out.write(buf, 0, t);
          t = is.read(buf);
        }
        if (!tmpDst.renameTo(dstFile)) {
          throw new IOException(
              "Failed to rename " + tmpDst.getPath() + " to destination " + dstFile.getPath());
        }
        System.out.println("Copied " + srcPath + " to " + dstFile.getPath());
      } finally {
        closer.close();
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Displays the number of folders and files matching the specified prefix in argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void count(TachyonURI path) throws IOException {
    long[] values = countHelper(path);
    String format = "%-25s%-25s%-15s%n";
    System.out.format(format, "File Count", "Folder Count", "Total Bytes");
    System.out.format(format, values[0], values[1], values[2]);
  }

  private long[] countHelper(TachyonURI path) throws IOException {
    TachyonFile fd;
    FileInfo fInfo;
    try {
      fd = mTfs.open(path);
      fInfo = mTfs.getInfo(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (!fInfo.isFolder) {
      return new long[] {1L, 0L, fInfo.length};
    }

    long[] rtn = new long[] {0L, 1L, 0L};

    List<FileInfo> files = null;
    try {
      files = mTfs.listStatus(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    Collections.sort(files);
    for (FileInfo file : files) {
      long[] toAdd = countHelper(new TachyonURI(file.getPath()));
      rtn[0] += toAdd[0];
      rtn[1] += toAdd[1];
      rtn[2] += toAdd[2];
    }
    return rtn;
  }

  /**
   * Displays the file's all blocks info
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void fileinfo(TachyonURI path) throws IOException {
    TachyonFile fd;
    FileInfo fInfo;
    try {
      fd = mTfs.open(path);
      fInfo = mTfs.getInfo(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (fInfo.isFolder) {
      throw new IOException(path + " is a directory path so does not have file blocks.");
    }

    System.out.println(path + " with file id " + fd.getFileId() + " has the following blocks: ");
    for (long blockId : fInfo.getBlockIds()) {
      System.out.println(TachyonBlockStore.get().getInfo(blockId));
    }
  }

  /**
   * Displays a list of hosts that have the file specified in argv stored.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void location(TachyonURI path) throws IOException {
    TachyonFile fd;
    FileInfo fInfo;
    try {
      fd = mTfs.open(path);
      fInfo = mTfs.getInfo(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    System.out.println(path + " with file id " + fd.getFileId() + " is on nodes: ");
    for (long blockId : fInfo.getBlockIds()) {
      for (BlockLocation location : TachyonBlockStore.get().getInfo(blockId).getLocations()) {
        System.out.println(location.getWorkerAddress().getHost());
      }
    }
  }

  private List<FileInfo> listStatusSortedByIncreasingCreationTime(TachyonURI path)
      throws IOException {
    List<FileInfo> files = null;
    try {
      TachyonFile fd = mTfs.open(path);
      files = mTfs.listStatus(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    Collections.sort(files, new Comparator<FileInfo>() {
      @Override
      public int compare(FileInfo fileInfo, FileInfo fileInfo2) {
        long t1 = fileInfo.creationTimeMs;
        long t2 = fileInfo2.creationTimeMs;
        if (t1 < t2) {
          return -1;
        }
        if (t1 == t2) {
          return 0;
        }
        return 1;
      }
    });
    return files;
  }

  public static Comparator<TachyonURI> createTachyonURIComparator() {
    return new Comparator<TachyonURI>() {
      @Override
      public int compare(TachyonURI tUri1, TachyonURI tUri2) {
        // ascending order
        return tUri1.getPath().compareTo(tUri2.getPath());
      }
    };
  }

  /**
   * Displays information for all directories and files directly under the path specified in argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void ls(TachyonURI path) throws IOException {
    List<FileInfo> files = listStatusSortedByIncreasingCreationTime(path);
    String format = "%-10s%-25s%-15s%-5s%n";
    for (FileInfo file : files) {
      String inMemory = "";
      if (!file.isFolder) {
        if (100 == file.inMemoryPercentage) {
          inMemory = "In Memory";
        } else {
          inMemory = "Not In Memory";
        }
      }
      System.out.format(format, FormatUtils.getSizeFromBytes(file.getLength()),
          convertMsToDate(file.getCreationTimeMs()), inMemory, file.getPath());
    }
  }

  public static String convertMsToDate(long Millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:SSS");
    return formatter.format(new Date(Millis));
  }

  /**
   * Displays information for all directories and files under the path specified in argv
   * recursively.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void lsr(TachyonURI path) throws IOException {
    List<FileInfo> files = listStatusSortedByIncreasingCreationTime(path);
    String format = "%-10s%-25s%-15s%-5s%n";
    for (FileInfo file : files) {
      String inMemory = "";
      if (!file.isFolder) {
        if (100 == file.inMemoryPercentage) {
          inMemory = "In Memory";
        } else {
          inMemory = "Not In Memory";
        }
      }
      System.out.format(format, FormatUtils.getSizeFromBytes(file.getLength()),
          convertMsToDate(file.getCreationTimeMs()), inMemory, file.getPath());
      if (file.isFolder) {
        lsr(new TachyonURI(path.getScheme(), path.getAuthority(), file.getPath()));
      }
    }
  }

  /**
   * Creates a new directory specified by the path in argv, including any parent folders that are
   * required. This method fails if a directory or file with the same path already exists.
   *
   * @param path The TachyonURI path as the input of the command
   */
  public void mkdir(TachyonURI path) throws IOException {
    try {
      MkdirOptions options = new MkdirOptions.Builder(mTachyonConf).setRecursive(true).build();
      mTfs.mkdir(path, options);
      System.out.println("Successfully created directory " + path);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Get number of bytes used in the TachyonFileSystem
   *
   */
  public void getUsedBytes() throws IOException {
    long usedBytes = TachyonBlockStore.get().getUsedBytes();
    System.out.println("Used Bytes: " + usedBytes);
  }

  /**
   * Get the capacity of the TachyonFileSystem
   *
   */
  public void getCapacityBytes() throws IOException {
    long capacityBytes = TachyonBlockStore.get().getCapacityBytes();
    System.out.println("Capacity Bytes: " + capacityBytes);
  }

  /**
   * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
   * never evicted from memory.
   *
   * @param path The TachyonURI path as the input of the command
   */
  public void pin(TachyonURI path) throws IOException {
    try {
      TachyonFile fd = mTfs.open(path);
      SetStateOptions options = new SetStateOptions.Builder(mTachyonConf).setPinned(true).build();
      mTfs.setState(fd, options);
      System.out.println("File '" + path + "' was successfully pinned.");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Method which prints the method to use all the commands.
   */
  public void printUsage() {
    System.out.println("Usage: java TfsShell");
    System.out.println("       [cat <path>]");
    System.out.println("       [copyFromLocal <src> <remoteDst>]");
    System.out.println("       [copyToLocal <src> <localDst>]");
    System.out.println("       [count <path>]");
    System.out.println("       [du <path>]");
    System.out.println("       [fileinfo <path>]");
    System.out.println("       [free <file path|folder path>]");
    System.out.println("       [getUsedBytes]");
    System.out.println("       [getCapacityBytes]");
    System.out.println("       [load <path>]");
    System.out.println("       [loadMetadata <path>]");
    System.out.println("       [location <path>]");
    System.out.println("       [ls <path>]");
    System.out.println("       [lsr <path>]");
    System.out.println("       [mkdir <path>]");
    System.out.println("       [mount <tachyonPath> <ufsURI>]");
    System.out.println("       [mv <src> <dst>]");
    System.out.println("       [pin <path>]");
    System.out.println("       [report <path>]");
    System.out.println("       [request <tachyonaddress> <dependencyId>]");
    System.out.println("       [rm <path>]");
    System.out.println("       [rmr <path>]");
    System.out.println("       [tail <path>]");
    System.out.println("       [touch <path>]");
    System.out.println("       [unmount <tachyonPath>]");
    System.out.println("       [unpin <path>]");
  }

  /**
   * Return the number of arguments a command should have
   *
   * @param cmd The command
   * @return The number of argument of the input command
   */
  public int getNumOfArgs(String cmd) {
    if (cmd.equals("getUsedBytes") || cmd.equals("getCapacityBytes")
        || cmd.equals("listLineages")) {
      return 0;
    } else if (cmd.equals("cat") || cmd.equals("count") || cmd.equals("ls") || cmd.equals("lsr")
        || cmd.equals("mkdir") || cmd.equals("rm") || cmd.equals("rmr") || cmd.equals("tail")
        || cmd.equals("touch") || cmd.equals("load") || cmd.equals("fileinfo")
        || cmd.equals("location") || cmd.equals("report") || cmd.equals("pin")
        || cmd.equals("unpin") || cmd.equals("free") || cmd.equals("du") || cmd.equals("unmount")
        || cmd.equals("loadMetadata")) {
      return 1;
    } else if (cmd.equals("copyFromLocal") || cmd.equals("copyToLocal") || cmd.equals("request")
        || cmd.equals("mount") || cmd.equals("mv") || cmd.equals("deleteLineage")) {
      return 2;
    } else if (cmd.equals("createLineage")) {
      return 3;
    } else {
      return -1;
    }
  }

  /**
   * Mounts a UFS path onto a Tachyon path.
   *
   * @param argv Aaray of arguments given by the user's input from the terminal
   * @throws IOException if an I/O error occurs
   */
  public void mount(String[] argv) throws IOException {
    TachyonURI tachyonPath = new TachyonURI(argv[1]);
    TachyonURI ufsPath = new TachyonURI(argv[2]);
    try {
      if (mTfs.mount(tachyonPath, ufsPath)) {
        System.out.println("Mounted " + ufsPath + " at " + tachyonPath);
      } else {
        throw new IOException("mount: Failed to mount" + ufsPath + " to " + tachyonPath);
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Unmounts a Tachyon path.
   *
   * @param path the TachyonURI path as the input of the command
   * @throws IOException if an I/O error occurs
   */
  public void unmount(TachyonURI path) throws IOException {
    try {
      if (mTfs.unmount(path)) {
        System.out.println("Unmounted " + path);
      } else {
        throw new IOException("unmount: Failed to unmount" + path);
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Loads metadata for the given Tachyon path from UFS.
   *
   * @param path the TachyonURI path as the input of the command
   * @throws IOException if an I/O error occurs
   */
  public void loadMetadata(TachyonURI path) throws IOException {
    try {
      LoadMetadataOptions recursive =
          new LoadMetadataOptions.Builder(mTachyonConf).setRecursive(true).build();
      mTfs.loadMetadata(path, recursive);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Renames a file or directory specified by argv. Will fail if the new path name already exists.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @throws IOException
   */
  public void rename(String[] argv) throws IOException {
    TachyonURI srcPath = new TachyonURI(argv[1]);
    TachyonURI dstPath = new TachyonURI(argv[2]);
    try {
      TachyonFile fd = mTfs.open(srcPath);
      if (mTfs.rename(fd, dstPath)) {
        System.out.println("Renamed " + srcPath + " to " + dstPath);
      } else {
        throw new IOException("mv: Failed to rename " + srcPath + " to " + dstPath);
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  public void report(TachyonURI path) throws IOException {
    try {
      TachyonFile fd = mTfs.open(path);
      mTfs.reportLostFile(fd);
      System.out
          .println(path + " with file id " + fd.getFileId() + " has reported been report lost.");
      listLineages();
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Removes the file specified by argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void rm(TachyonURI path) throws IOException {
    TachyonFile fd;
    FileInfo fInfo;
    try {
      fd = mTfs.open(path);
      fInfo = mTfs.getInfo(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (fInfo.isFolder) {
      throw new IOException("rm: cannot remove a directory, please try rmr <path>");
    }

    try {
      mTfs.delete(fd);
      System.out.println(path + " has been removed");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Removes the file or directory specified by argv. Will remove all files and directories in the
   * directory if a directory is specified.
   *
   * @param path The TachyonURI path as the input of the command
   */
  public void rmr(TachyonURI path) throws IOException {
    try {
      DeleteOptions options = new DeleteOptions.Builder(mTachyonConf).setRecursive(true).build();
      TachyonFile fd = mTfs.open(path);
      mTfs.delete(fd, options);
      System.out.println(path + " has been removed");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Displays the size of a file or a directory specified by argv.
   *
   * @param path The TachyonURI path as the input of the command
   */
  public void du(TachyonURI path) throws IOException {
    long sizeInBytes = getFileOrFolderSize(mTfs, path);
    System.out.println(path + " is " + sizeInBytes + " bytes");
  }

  /**
   * Method which determines how to handle the user's request, will display usage help to the user
   * if command format is incorrect.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   */
  public int run(String[] argv) {
    if (argv.length == 0) {
      printUsage();
      return -1;
    }

    // Sanity check on the number of arguments
    String cmd = argv[0];
    int numOfArgs = getNumOfArgs(cmd);

    if (numOfArgs == -1) { // Unknown command (we didn't find the cmd in our dict)
      System.out.println(cmd + " is an unknown command.\n");
      printUsage();
      return -1;
    }

    // FIXME(yupeng) remove the first condition
    if (numOfArgs < 3 && numOfArgs != argv.length - 1) {
      System.out.println(
          cmd + " takes " + numOfArgs + " arguments, " + " not " + (argv.length - 1) + "\n");
      printUsage();
      return -1;
    }

    // Handle the command
    try {
      if (numOfArgs == 0) { // commands need 0 argument
        if (cmd.equals("getUsedBytes")) {
          getUsedBytes();
        } else if (cmd.equals("getCapacityBytes")) {
          getCapacityBytes();
        } else if (cmd.equals("listLineages")) {
          return listLineages();
        }
      } else if (numOfArgs == 1) { // commands need 1 argument
        TachyonURI inputPath = new TachyonURI(argv[1]);

        // mkdir & touch & count does not support wildcard by semantics
        if (cmd.equals("mkdir")) {
          mkdir(inputPath);
        } else if (cmd.equals("touch")) {
          touch(inputPath);
        } else if (cmd.equals("count")) {
          count(inputPath);
        } else if (cmd.equals("unmount")) {
          unmount(inputPath);
        } else if (cmd.equals("loadMetadata")) {
          loadMetadata(inputPath);
        } else {
          List<TachyonURI> paths = null;
          paths = TfsShellUtils.getTachyonURIs(mTfs, inputPath);
          if (paths.size() == 0) { // A unified sanity check on the paths
            throw new IOException(inputPath + " does not exist.");
          }
          Collections.sort(paths, createTachyonURIComparator());

          List<String> errorMessages = new ArrayList<String>();
          for (TachyonURI path : paths) {
            try {
              if (cmd.equals("cat")) {
                cat(path);
              } else if (cmd.equals("ls")) {
                ls(path);
              } else if (cmd.equals("lsr")) {
                lsr(path);
              } else if (cmd.equals("rm")) {
                rm(path);
              } else if (cmd.equals("rmr")) {
                rmr(path);
              } else if (cmd.equals("tail")) {
                tail(path);
              } else if (cmd.equals("load")) {
                load(path);
              } else if (cmd.equals("fileinfo")) {
                fileinfo(path);
              } else if (cmd.equals("location")) {
                location(path);
              } else if (cmd.equals("report")) {
                report(path);
              } else if (cmd.equals("pin")) {
                pin(path);
              } else if (cmd.equals("unpin")) {
                unpin(path);
              } else if (cmd.equals("free")) {
                free(path);
              } else if (cmd.equals("du")) {
                du(path);
              }
            } catch (IOException e) {
              errorMessages.add(e.getMessage());
            }
          }
          if (errorMessages.size() != 0) {
            throw new IOException(Joiner.on('\n').join(errorMessages));
          }
        }
      } else if (numOfArgs == 2) { // commands need 2 arguments
        if (cmd.equals("copyFromLocal")) {
          String srcPath = argv[1];
          TachyonURI dstPath = new TachyonURI(argv[2]);
          List<File> srcFiles = TfsShellUtils.getFiles(srcPath);
          if (srcFiles.size() == 0) {
            throw new IOException("Local path " + srcPath + " does not exist.");
          }

          if (srcPath.contains(TachyonURI.WILDCARD)) {
            copyFromLocalWildcard(srcFiles, dstPath);
          } else {
            copyFromLocal(new File(srcPath), dstPath);
          }
        } else if (cmd.equals("copyToLocal")) {
          TachyonURI srcPath = new TachyonURI(argv[1]);
          File dstFile = new File(argv[2]);
          List<TachyonURI> srcPaths = TfsShellUtils.getTachyonURIs(mTfs, srcPath);
          if (srcPaths.size() == 0) {
            throw new IOException(srcPath.getPath() + " does not exist.");
          }

          if (srcPath.containsWildcard()) {
            copyWildcardToLocal(srcPaths, dstFile);
          } else {
            copyToLocal(srcPath, dstFile);
          }
        } else if (cmd.equals("mv")) {
          rename(argv);
        } else if (cmd.equals("deleteLineage")) {
          deleteLineage(argv);
        } else if (cmd.equals("mount")) {
          mount(argv);
        }
      } else if (numOfArgs > 2) { // commands need 3 arguments and more
        if (cmd.equals("createLineage")) {
          createLineage(argv);
        }
      }
      return 0;
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
      return -1;
    }
  }

  /**
   * Prints the file's last 1KB of contents to the console.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void tail(TachyonURI path) throws IOException {
    TachyonFile fd;
    FileInfo fInfo;
    try {
      fd = mTfs.open(path);
      fInfo = mTfs.getInfo(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (!fInfo.isFolder) {
      InStreamOptions op = new InStreamOptions.Builder(mTachyonConf)
          .setTachyonStorageType(TachyonStorageType.NO_STORE).build();
      FileInStream is = null;
      try {
        is = mTfs.getInStream(fd, op);
        byte[] buf = new byte[Constants.KB];
        long bytesToRead = 0L;
        if (fInfo.getLength() > Constants.KB) {
          bytesToRead = Constants.KB;
        } else {
          bytesToRead = fInfo.getLength();
        }
        is.skip(fInfo.getLength() - bytesToRead);
        int read = is.read(buf);
        if (read != -1) {
          System.out.write(buf, 0, read);
        }
      } catch (TachyonException e) {
        throw new IOException(e.getMessage());
      } finally {
        is.close();
      }
    } else {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
  }

  /**
   * Creates a 0 byte file specified by argv. The file will be written to UnderFileSystem.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void touch(TachyonURI path) throws IOException {
    try {
      mTfs.getOutStream(path, new OutStreamOptions.Builder(mTachyonConf)
          .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build()).close();
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    System.out.println(path + " has been created");
  }

  /**
   * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
   * are never evicted from memory, so this method will allow such files to be evicted.
   *
   * @param path The TachyonURI path as the input of the command
   * @throws IOException
   */
  public void unpin(TachyonURI path) throws IOException {
    try {
      SetStateOptions options = new SetStateOptions.Builder(mTachyonConf).setPinned(false).build();
      TachyonFile fd = mTfs.open(path);
      mTfs.setState(fd, options);
      System.out.println("File '" + path + "' was successfully unpinned.");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Free the given file or folder from tachyon in-memory (recursively freeing all children if a
   * folder)
   *
   * @param path The TachyonURI path as the input of the command
   */
  public void free(TachyonURI path) throws IOException {
    try {
      FreeOptions options = new FreeOptions.Builder(mTachyonConf).setRecursive(true).build();
      TachyonFile fd = mTfs.open(path);
      mTfs.free(fd, options);
      System.out.println(path + " was successfully freed from memory.");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Calculates the size of a path (file or folder) specified by a TachyonURI.
   *
   * @param tachyonFS A TachyonFileSystem
   * @param path A TachyonURI denoting the path
   * @return total size of the specified path in byte
   * @throws IOException
   */
  private long getFileOrFolderSize(TachyonFileSystem tachyonFS, TachyonURI path)
      throws IOException {
    long sizeInBytes = 0;
    List<FileInfo> files;
    try {
      TachyonFile inputFile = tachyonFS.open(path);
      files = tachyonFS.listStatus(inputFile);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    for (FileInfo file : files) {
      if (file.isFolder) {
        TachyonURI subFolder = new TachyonURI(file.getPath());
        sizeInBytes += getFileOrFolderSize(tachyonFS, subFolder);
      } else {
        sizeInBytes += file.getLength();
      }
    }
    return sizeInBytes;
  }

  private void createLineage(String[] argv) throws IOException {
    TachyonLineage tl = TachyonLineage.get();
    // TODO(yupeng) more validation
    List<TachyonURI> inputFiles = Lists.newArrayList();
    if (!argv[1].equals("noInput")) {
      for (String path : argv[1].split(",")) {
        inputFiles.add(new TachyonURI(path));
      }
    }
    List<TachyonURI> outputFiles = Lists.newArrayList();
    for (String path : argv[2].split(",")) {
      outputFiles.add(new TachyonURI(path));
    }
    String cmd = "";
    for (int i = 3; i < argv.length; i ++) {
      cmd += argv[i] + " ";
    }

    String outputPath = ClientContext.getConf().get(Constants.MASTER_LINEAGE_RECOMPUTE_LOG_PATH);
    if (outputPath == null) {
      throw new IOException("recompute output log is not configured");
    }
    CommandLineJob job = new CommandLineJob(cmd, new JobConf(outputPath));
    long lineageId;
    try {
      lineageId = tl.createLineage(inputFiles, outputFiles, job);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    System.out.println("Lineage " + lineageId + " has been created.");
  }

  private int deleteLineage(String[] argv) throws IOException {
    TachyonLineage tl = TachyonLineage.get();
    long lineageId = Long.parseLong(argv[1]);
    boolean cascade = Boolean.parseBoolean(argv[2]);
    DeleteLineageOptions options =
        new DeleteLineageOptions.Builder(new TachyonConf()).setCascade(cascade).build();
    try {
      tl.deleteLineage(lineageId, options);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Lineage '" + lineageId + "' could not be deleted.");
    }
    System.out.println("Lineage " + lineageId + " has been deleted.");
    return 0;
  }

  private int listLineages() throws IOException {
    TachyonLineage tl = TachyonLineage.get();
    List<LineageInfo> infos = tl.getLineageInfoList();
    for (LineageInfo info : infos) {
      System.out.println(info);
    }
    return 0;
  }
}
