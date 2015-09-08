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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

/**
 * Class for handling command line inputs.
 */
public class TFsShell implements Closeable {
  /**
   * Main method, starts a new TFsShell
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    TFsShell shell = new TFsShell(new TachyonConf());
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

  public TFsShell(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mCloser = Closer.create();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * Prints the file's contents to the console.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int cat(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);

    if (tFile == null) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
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
      return 0;
    } else {
      System.out.println(path + " is not a file.");
      return -1;
    }
  }

  /**
   * Load a file or directory in Tachyon space, makes it resident in memory.
   *
   * @param filePath The TachyonURI path to load into Tachyon memory
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int load(TachyonURI filePath) throws IOException {
    TachyonFS tachyonClient = createFS(filePath);
    int ret = loadPath(tachyonClient, filePath);
    if (ret == 0) {
      System.out.println(filePath + " loaded");
    } else {
      System.out.println("Loading " + filePath + " failed");
    }
    return ret;
  }

  private int loadPath(TachyonFS tachyonClient, TachyonURI filePath) throws IOException {
    TachyonFile tFile = tachyonClient.getFile(filePath);
    if (tFile == null) {
      return -1;
    }
    if (tFile.isDirectory()) {
      List<ClientFileInfo> files = tachyonClient.listStatus(filePath);
      Collections.sort(files);
      for (ClientFileInfo file : files) {
        TachyonURI newPath = new TachyonURI(file.getPath());
        if (loadPath(tachyonClient, newPath) == -1) {
          return -1;
        }
      }
      return 0;
    } else {
      Closer closer = Closer.create();
      InStream in = closer.register(tFile.getInStream(ReadType.CACHE));
      byte[] buf = new byte[8 * Constants.MB];
      try {
        while (in.read(buf) != -1) {
        }
        return 0;
      } catch (Throwable e) {
        throw closer.rethrow(e);
      } finally {
        closer.close();
      }
    }
  }

  /**
   * Copies a list of files or directories specified by srcFiles from the local filesystem to 
   * dstPath in the Tachyon filesystem space. 
   * This method is used when the input path contains wildcards.
   * @param srcFiles The list of files in the local filesystem
   * @param dstPath The TachyonURI of the destination
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int copyFromLocalWildcard(List<File> srcFiles, TachyonURI dstPath) throws IOException {
    TachyonFS tachyonClient = createFS(dstPath);
    TachyonFile dstFile = tachyonClient.getFile(dstPath);
    if (dstFile != null && !dstFile.isDirectory()) {
      System.out.println("The destination cannot be an existent file when the src contains " 
          + "wildcards.");
      return -1;
    }
    if (dstFile == null) {
      if (!tachyonClient.mkdirs(dstPath, true)) {
        System.out.print("Fail to create directory: " + dstPath);
        return -1;
      } else {
        System.out.println("Create directory: " + dstPath);
      }
    }
    int exitCode = 0;
    for (File srcFile : srcFiles) {
      try {
        exitCode |= copyFromLocal(srcFile,
            new TachyonURI(PathUtils.concatPath(dstPath.getPath(), srcFile.getName())));
      } catch (IOException ioe) {
        System.out.println(ioe.getMessage());
        exitCode |= -1;
      }
    }
    return exitCode;
  }
  
  /**
   * Copies a file or directory specified by srcPath from the local filesystem to dstPath in 
   * the Tachyon filesystem space. Will
   * fail if the path given already exists in the filesystem.
   *
   * @param srcFile The source file in the local filesystem
   * @param dstPath The TachyonURI of the destination
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int copyFromLocal(File srcFile, TachyonURI dstPath) throws IOException {
    TachyonFS tachyonClient = createFS(dstPath);
    int ret = copyPath(srcFile, tachyonClient, dstPath);
    if (ret == 0) {
      System.out.println("Copied " + srcFile.getPath() + " to " + dstPath);
    }
    return ret;
  }

  private int copyPath(File src, TachyonFS tachyonClient, TachyonURI dstPath) throws IOException {
    if (!src.isDirectory()) {
      TachyonFile tFile = tachyonClient.getFile(dstPath);
      if (tFile != null && tFile.isDirectory()) {
        dstPath = dstPath.join(src.getName());
      }
      int fileId = tachyonClient.createFile(dstPath);
      if (fileId == -1) {
        return -1;
      }
      tFile = tachyonClient.getFile(fileId);
      Closer closer = Closer.create();
      try {
        WriteType writeType =
            mTachyonConf.getEnum(Constants.USER_DEFAULT_WRITE_TYPE, WriteType.CACHE_THROUGH);
        OutStream os = closer.register(tFile.getOutStream(writeType));
        FileInputStream in = closer.register(new FileInputStream(src));
        FileChannel channel = closer.register(in.getChannel());
        ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
        while (channel.read(buf) != -1) {
          buf.flip();
          os.write(buf.array(), 0, buf.limit());
        }
      } finally {
        closer.close();
      }
      return 0;
    } else {
      tachyonClient.mkdir(dstPath);
      for (String file : src.list()) {
        TachyonURI newPath = new TachyonURI(dstPath, new TachyonURI(file));
        File srcFile = new File(src, file);
        if (copyPath(srcFile, tachyonClient, newPath) == -1) {
          return -1;
        }
      }
    }
    return 0;
  }

  /**
   * Copies a list of files or directories specified by srcPaths from the Tachyon filesystem to 
   * dstPath in the local filesystem. 
   * This method is used when the input path contains wildcards.
   * 
   * @param srcPaths The list of files in the Tachyon filesystem
   * @param dstFile The destination directory in the local filesystem 
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int copyWildcardToLocal(List<TachyonURI> srcPaths, File dstFile) throws IOException {
    if (dstFile.exists() && !dstFile.isDirectory()) {
      System.out.println("The destination cannot be an existent file when the src contains " 
          + "wildcards.");
      return -1;
    }
    if (!dstFile.exists()) {
      if (!dstFile.mkdirs()) {
        System.out.print("Fail to create directory: " + dstFile.getPath());
        return -1;
      } else {
        System.out.println("Create directory: " + dstFile.getPath());
      }
    }
    int exitCode = 0;
    for (TachyonURI srcPath : srcPaths) {
      try {
        copyToLocal(srcPath, new File(dstFile.getAbsoluteFile(), srcPath.getName()));
      } catch (IOException ioe) {
        System.out.println(ioe.getMessage());
        exitCode |= -1;
      }
    }
    return exitCode;
  }
  
  /**
   * Copies a file or a directory from the Tachyon filesystem to the local filesystem.
   * 
   * @param srcPath The source TachyonURI (could be a file or a directory)
   * @param dstFile The destination file in the local filesystem 
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int copyToLocal(TachyonURI srcPath, File dstFile) throws IOException {
    TachyonFS tachyonClient = createFS(srcPath);
    return copyToLocal(tachyonClient, srcPath, dstFile);
  }

  /**
   * Copies a file or a directory from the Tachyon filesystem to the local filesystem.
   * This is the utility function.
   * 
   * @param tachyonClient The TachyonFS to talk to
   * @param srcPath The source TachyonURI (could be a file or a directory)
   * @param dstFile The destination file in the local filesystem 
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  private int copyToLocal(TachyonFS tachyonClient, TachyonURI srcPath, File dstFile) 
      throws IOException {
    TachyonFile tFile = tachyonClient.getFile(srcPath);
    
    // tachyonClient.getFile() catches FileDoesNotExist exceptions and returns null
    if (tFile == null) {
      throw new IOException(srcPath.toString());
    }
    
    if (tFile.isDirectory()) {
      //make a local directory
      if (!dstFile.exists()) {
        if (!dstFile.mkdirs()) {
          System.out.println("mkdirs failure for directory: " + dstFile.getAbsolutePath());
          return -1;
        } else {
          System.out.println("Create directory: " + dstFile.getAbsolutePath());
        }
      }
      
      int ret = 0;
      List<ClientFileInfo> files = tachyonClient.listStatus(srcPath);
      for (ClientFileInfo file : files) {
        ret |= copyToLocal(tachyonClient, 
            new TachyonURI(srcPath.getScheme(), srcPath.getAuthority(), file.getPath()), 
            new File(dstFile.getAbsolutePath(), file.getName()));
      }
      return ret;
    } else {
      return copyFileToLocal(tachyonClient, srcPath, dstFile);
    }
  }
  
  /**
   * Copies a file from the Tachyon filesystem to the local filesystem.
   * This is the utility function.
   * 
   * @param tachyonClient The TachyonFS to talk to
   * @param srcPath The source TachyonURI (has to be a file)
   * @param dstFile The destination file in the local filesystem 
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  private int copyFileToLocal(TachyonFS tachyonClient, TachyonURI srcPath, File dstFile) 
      throws IOException {
    TachyonFile tFile = tachyonClient.getFile(srcPath);
    Closer closer = Closer.create();
    try {
      InStream is = closer.register(tFile.getInStream(ReadType.NO_CACHE));
      FileOutputStream out = closer.register(new FileOutputStream(dstFile));
      byte[] buf = new byte[64 * Constants.MB];
      int t = is.read(buf);
      while (t != -1) {
        out.write(buf, 0, t);
        t = is.read(buf);
      }
      System.out.println("Copied " + srcPath + " to " + dstFile.getPath());
      return 0;
    } finally {
      closer.close();
    }
  }

  /**
   * Displays the number of folders and files matching the specified prefix in argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int count(TachyonURI path) throws IOException {
    try {
      long[] values = countHelper(path);
      String format = "%-25s%-25s%-15s%n";
      System.out.format(format, "File Count", "Folder Count", "Total Bytes");
      System.out.format(format, values[0], values[1], values[2]);
    } catch (FileDoesNotExistException e) {
      System.out.println(e.getMessage() + " does not exist.");
      return -1;
    }
    return 0;
  }

  private long[] countHelper(TachyonURI path) throws FileDoesNotExistException, IOException {
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);
    if (tFile == null) {
      throw new FileDoesNotExistException(path.toString());
    }

    if (tFile.isFile()) {
      return new long[] {1L, 0L, tFile.length()};
    }

    long[] rtn = new long[] {0L, 1L, 0L};

    List<ClientFileInfo> files = tachyonClient.listStatus(path);
    Collections.sort(files);
    for (ClientFileInfo file : files) {
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
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int fileinfo(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.getFile(path).isDirectory()) {
      System.out.println(path + " is a directory path so does not have file blocks.");
      return -1;
    }
    int fileId = tachyonClient.getFileId(path);
    if (fileId == -1) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    List<ClientBlockInfo> blocks = tachyonClient.getFileBlocks(fileId);
    System.out.println(path + " with file id " + fileId + " has the following blocks: ");
    for (ClientBlockInfo block : blocks) {
      System.out.println(block);
    }
    return 0;
  }

  /**
   * Displays a list of hosts that have the file specified in argv stored.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int location(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    if (fileId == -1) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    List<String> hosts = tachyonClient.getFile(fileId).getLocationHosts();
    System.out.println(path + " with file id " + fileId + " is on nodes: ");
    for (String host : hosts) {
      System.out.println(host);
    }
    return 0;
  }

  /**
   * Displays information for all directories and files directly under the path specified in argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int ls(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    List<ClientFileInfo> files = tachyonClient.listStatus(path);
    Collections.sort(files);
    String format = "%-10s%-25s%-15s%-5s%n";
    for (ClientFileInfo file : files) {
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
    return 0;
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
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int lsr(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    List<ClientFileInfo> files = tachyonClient.listStatus(path);
    Collections.sort(files);
    String format = "%-10s%-25s%-15s%-5s%n";
    for (ClientFileInfo file : files) {
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
    return 0;
  }

  /**
   * Creates a new directory specified by the path in argv, including any parent folders that are
   * required. This method fails if a directory or file with the same path already exists.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int mkdir(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.mkdir(path)) {
      System.out.println("Successfully created directory " + path);
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Get number of bytes used in the TachyonFS
   *
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int getUsedBytes() throws IOException {
    TachyonURI path = new TachyonURI(TachyonURI.SEPARATOR);
    TachyonFS tachyonClient = createFS(path);
    long usedBytes = tachyonClient.getUsedBytes();
    if (usedBytes == -1) {
      return -1;
    }
    System.out.println("Used Bytes: " + usedBytes);
    return 0;
  }

  /**
   * Get the capacity of the TachyonFS
   *
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int getCapacityBytes() throws IOException {
    TachyonURI path = new TachyonURI(TachyonURI.SEPARATOR);
    TachyonFS tachyonClient = createFS(path);
    long capacityBytes = tachyonClient.getCapacityBytes();
    if (capacityBytes == -1) {
      return -1;
    }
    System.out.println("Capacity Bytes: " + capacityBytes);
    return 0;
  }

  /**
   * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
   * never evicted from memory.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int pin(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);

    try {
      tachyonClient.pinFile(fileId);
      System.out.println("File '" + path + "' was successfully pinned.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("File '" + path + "' could not be pinned.");
      return -1;
    }
  }

  /**
   * Method which prints the method to use all the commands.
   */
  public void printUsage() {
    System.out.println("Usage: java TFsShell");
    System.out.println("       [cat <path>]");
    System.out.println("       [count <path>]");
    System.out.println("       [ls <path>]");
    System.out.println("       [lsr <path>]");
    System.out.println("       [mkdir <path>]");
    System.out.println("       [rm <path>]");
    System.out.println("       [rmr <path>]");
    System.out.println("       [tail <path>]");
    System.out.println("       [touch <path>]");
    System.out.println("       [mv <src> <dst>]");
    System.out.println("       [load <path>]");
    System.out.println("       [copyFromLocal <src> <remoteDst>]");
    System.out.println("       [copyToLocal <src> <localDst>]");
    System.out.println("       [fileinfo <path>]");
    System.out.println("       [location <path>]");
    System.out.println("       [report <path>]");
    System.out.println("       [request <tachyonaddress> <dependencyId>]");
    System.out.println("       [pin <path>]");
    System.out.println("       [unpin <path>]");
    System.out.println("       [free <file path|folder path>]");
    System.out.println("       [getUsedBytes]");
    System.out.println("       [getCapacityBytes]");
    System.out.println("       [du <path>]");
  }

  /**
   * Return the number of arguments a command should have
   * @param cmd The command
   * @return The number of argument of the input command
   */
  public int getNumOfArgs(String cmd) {
    if (cmd.equals("getUsedBytes")
        || cmd.equals("getCapacityBytes")) {
      return 0;
    } else if (cmd.equals("cat")
        || cmd.equals("count")
        || cmd.equals("ls")
        || cmd.equals("lsr")
        || cmd.equals("mkdir")
        || cmd.equals("rm")
        || cmd.equals("rmr")
        || cmd.equals("tail")
        || cmd.equals("touch")
        || cmd.equals("load")
        || cmd.equals("fileinfo")
        || cmd.equals("location")
        || cmd.equals("report")
        || cmd.equals("pin")
        || cmd.equals("unpin")
        || cmd.equals("free")
        || cmd.equals("du")) {
      return 1;
    } else if (cmd.equals("copyFromLocal") 
        || cmd.equals("copyToLocal") 
        || cmd.equals("request")
        || cmd.equals("mv")) {
      return 2;
    } else {
      return -1;
    }
  }

  /**
   * Renames a file or directory specified by argv. Will fail if the new path name already exists.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int rename(String[] argv) throws IOException {
    TachyonURI srcPath = new TachyonURI(argv[1]);
    TachyonURI dstPath = new TachyonURI(argv[2]);
    TachyonFS tachyonClient = createFS(srcPath);
    if (tachyonClient.rename(srcPath, dstPath)) {
      System.out.println("Renamed " + srcPath + " to " + dstPath);
      return 0;
    } else {
      System.out.println("mv: Failed to rename " + srcPath + " to " + dstPath);
      return -1;
    }
  }

  public int report(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    tachyonClient.reportLostFile(fileId);
    System.out.println(path + " with file id " + fileId + " has reported been report lost.");
    return 0;
  }

  public int request(String[] argv) throws IOException {
    TachyonURI path = new TachyonURI(argv[1]);
    int depId = Integer.parseInt(argv[2]);
    TachyonFS tachyonClient = createFS(path);
    tachyonClient.requestFilesInDependency(depId);
    System.out.println("Dependency with ID " + depId + " has been requested.");
    return 0;
  }

  /**
   * Removes the file specified by argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int rm(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);
    if (tFile == null) {
      System.out.println("rm: cannot remove '" + path + "': No such file or directory");
      return -1;
    }
    if (tFile.isDirectory()) {
      System.out.println("rm: cannot remove a directory, please try rmr <path>");
      return -1;
    }
    if (tachyonClient.delete(path, false)) {
      System.out.println(path + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Removes the file or directory specified by argv. Will remove all files and directories in the
   * directory if a directory is specified.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int rmr(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.delete(path, true)) {
      System.out.println(path + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Displays the size of a file or a directory specified by argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int du(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.exist(path)) {
      long sizeInBytes = getFileOrFolderSize(tachyonClient, path);
      System.out.println(path + " is " + sizeInBytes + " bytes");
    } else {
      System.out.println(path + " does not exist");
    }
    return 0;
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
    
    if (numOfArgs != argv.length - 1) {
      System.out.println(cmd + " takes " + numOfArgs + " arguments.\n");
      printUsage();
      return -1;
    }
    
    // Handle the command
    try {
      if (numOfArgs == 0) { // commands need 0 argument
        if (cmd.equals("getUsedBytes")) {
          return getUsedBytes();
        } else if (cmd.equals("getCapacityBytes")) {
          return getCapacityBytes();
        }
      } else if (numOfArgs == 1) { // commands need 1 argument
        TachyonURI inputPath = new TachyonURI(argv[1]);
        
        // mkdir & touch & count does not support wildcard by semantics
        if (cmd.equals("mkdir")) {
          return mkdir(inputPath);
        } else if (cmd.equals("touch")) {
          return touch(inputPath);
        } else if (cmd.equals("count")) {
          return count(inputPath);
        }
        
        List<TachyonURI> paths = TFsShellUtils.getTachyonURIs(createFS(inputPath), inputPath);
        if (paths.size() == 0) { // A unified sanity check on the paths
          System.out.println(inputPath + " does not exist.");
          return -1;
        }
        
        int exitCode = 0;
        for (TachyonURI path : paths) {
          try {
            if (cmd.equals("cat")) {
              exitCode |= cat(path);
            } else if (cmd.equals("ls")) {
              exitCode |= ls(path);
            } else if (cmd.equals("lsr")) {
              exitCode |= lsr(path);
            } else if (cmd.equals("rm")) {
              exitCode |= rm(path);
            } else if (cmd.equals("rmr")) {
              exitCode |= rmr(path);
            } else if (cmd.equals("tail")) {
              exitCode |= tail(path);
            } else if (cmd.equals("load")) {
              exitCode |= load(path);
            } else if (cmd.equals("fileinfo")) {
              exitCode |= fileinfo(path);
            } else if (cmd.equals("location")) {
              exitCode |= location(path);
            } else if (cmd.equals("report")) {
              exitCode |= report(path);
            } else if (cmd.equals("pin")) {
              exitCode |= pin(path);
            } else if (cmd.equals("unpin")) {
              exitCode |= unpin(path);
            } else if (cmd.equals("free")) {
              exitCode |= free(path);
            } else if (cmd.equals("du")) {
              exitCode |= du(path);
            }
          } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
            exitCode |= -1;
          }
        }
        return exitCode;
        
      } else if (numOfArgs == 2) { // commands need 2 arguments
        if (cmd.equals("copyFromLocal")) {
          String srcPath = argv[1];
          TachyonURI dstPath = new TachyonURI(argv[2]);
          List<File> srcFiles = TFsShellUtils.getFiles(srcPath);
          if (srcFiles.size() == 0) {
            System.out.println("Local path " + srcPath + " does not exist.");
            return -1;
          }
          
          if (srcPath.contains(TachyonURI.WILDCARD)) {
            return copyFromLocalWildcard(srcFiles, dstPath);
          } else {
            return copyFromLocal(new File(srcPath), dstPath);
          }
        } else if (cmd.equals("copyToLocal")) {
          TachyonURI srcPath = new TachyonURI(argv[1]);
          File dstFile = new File(argv[2]);
          List<TachyonURI> srcPaths = TFsShellUtils.getTachyonURIs(createFS(srcPath), srcPath);
          if (srcPaths.size() == 0) {
            System.out.println(srcPath.getPath() + " does not exist.");
            return -1;
          }
          
          if (srcPath.containsWildcard()) {
            return copyWildcardToLocal(srcPaths, dstFile);
          } else {
            return copyToLocal(srcPath, dstFile);
          }
        } else if (cmd.equals("request")) {
          return request(argv);
        } else if (cmd.equals("mv")) {
          return rename(argv);
        }
      }
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
    }
    return -1;
  }   

  /**
   * Prints the file's last 1KB of contents to the console.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.f
   * @throws IOException
   */
  public int tail(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);

    if (tFile == null) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      try {
        byte[] buf = new byte[Constants.KB];
        long bytesToRead = 0L;
        if (tFile.length() > Constants.KB) {
          bytesToRead = Constants.KB;
        } else {
          bytesToRead = tFile.length();
        }
        is.skip(tFile.length() - bytesToRead);
        int read = is.read(buf);
        if (read != -1) {
          System.out.write(buf, 0, read);
        }
        return 0;
      } finally {
        is.close();
      }
    } else {
      System.out.println(path + " is not a file.");
      return -1;
    }
  }

  /**
   * Creates a 0 byte file specified by argv.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws IOException
   */
  public int touch(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(tachyonClient.createFile(path));
    OutputStream out = tFile.getOutStream(WriteType.THROUGH);
    out.close();
    System.out.println(path + " has been created");
    return 0;
  }

  /**
   * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
   * are never evicted from memory, so this method will allow such files to be evicted.
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int unpin(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    try {
      tachyonClient.unpinFile(fileId);
      System.out.println("File '" + path + "' was successfully unpinned.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("File '" + path + "' could not be unpinned.");
      return -1;
    }
  }

  /**
   * Free the file or Folder from tachyon in-memory specified by argv
   *
   * @param path The TachyonURI path as the input of the command
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws IOException
   */
  public int free(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.freepath(path, true)) {
      System.out.println(path + " was successfully freed from memory.");
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Creates a new TachyonFS and registers it with {@link #mCloser}
   */
  private TachyonFS createFS(final TachyonURI path) throws IOException {
    String qualifiedPath = TFsShellUtils.validatePath(path.toString(), mTachyonConf);
    TachyonFS tachyonFS = TachyonFS.get(new TachyonURI(qualifiedPath), mTachyonConf);
    return mCloser.register(tachyonFS);
  }

  /**
   * Calculates the size of a path (file or folder) specified by a TachyonURI.
   *
   * @param tachyonFS A TachyonFS
   * @param path A TachyonURI denoting the path
   * @return total size of the specified path in byte.
   * @throws IOException
   */
  private long getFileOrFolderSize(TachyonFS tachyonFS, TachyonURI path) throws IOException {
    long sizeInBytes = 0;
    List<ClientFileInfo> files = tachyonFS.listStatus(path);
    for (ClientFileInfo file : files) {
      if (file.isFolder) {
        TachyonURI subFolder = new TachyonURI(file.getPath());
        sizeInBytes += getFileOrFolderSize(tachyonFS, subFolder);
      } else {
        sizeInBytes += file.getLength();
      }
    }
    return sizeInBytes;
  }
}
