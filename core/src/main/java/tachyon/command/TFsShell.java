/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.CommonUtils;

/**
 * Class for handling command line inputs.
 */
public class TFsShell {
  /**
   * Main method, starts a new TFsShell
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   */
  public static void main(String argv[]) throws TException {
    TFsShell shell = new TFsShell();
    System.exit(shell.run(argv));
  }

  /**
   * Prints the file's contents to the console.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int cat(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs cat <path>");
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    TachyonFile tFile = tachyonClient.getFile(file);

    if (tFile == null) {
      System.out.println(file + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      byte[] buf = new byte[512];
      int read = is.read(buf);
      while (read != -1) {
        System.out.write(buf, 0, read);
        read = is.read(buf);
      }
      return 0;
    } else {
      System.out.println(file + " is not a file.");
      return -1;
    }
  }

  /**
   * Copies a file or directory specified by argv from the local filesystem to the filesystem. Will
   * fail if the path given already exists in the filesystem.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int copyFromLocal(String argv[]) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs copyFromLocal <src> <remoteDst>");
      return -1;
    }

    String srcPath = argv[1];
    String dstPath = argv[2];
    File src = new File(srcPath);
    if (!src.exists()) {
      System.out.println("Local path " + srcPath + " does not exist.");
      return -1;
    }
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(dstPath));
    String file = Utils.getFilePath(dstPath);
    int ret = copyPath(src, tachyonClient, file);
    if (ret == 0) {
      System.out.println("Copied " + src.getPath() + " to " + dstPath);
    }
    return ret;
  }

  private int copyPath(File src, TachyonFS tachyonClient, String dstPath) throws IOException {
    if (!src.isDirectory()) {
      int fileId = tachyonClient.createFile(dstPath);
      if (fileId == -1) {
        return -1;
      }
      TachyonFile tFile = tachyonClient.getFile(fileId);
      OutStream os = tFile.getOutStream(WriteType.CACHE_THROUGH);
      FileInputStream in = new FileInputStream(src);
      FileChannel channel = in.getChannel();
      ByteBuffer buf = ByteBuffer.allocate(Constants.KB);
      while (channel.read(buf) != -1) {
        buf.flip();
        os.write(buf.array(), 0, buf.limit());
      }
      os.close();
      channel.close();
      in.close();
      return 0;
    } else {
      tachyonClient.mkdir(dstPath);
      for (String file : src.list()) {
        String newPath = FilenameUtils.concat(dstPath, file);
        File srcFile = new File(src, file);
        if (copyPath(srcFile, tachyonClient, newPath) == -1) {
          return -1;
        }
      }
    }
    return 0;
  }

  /**
   * Copies a file specified by argv from the filesystem to the local filesystem.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int copyToLocal(String argv[]) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs copyToLocal <src> <localdst>");
      return -1;
    }

    String srcPath = argv[1];
    String dstPath = argv[2];
    String folder = Utils.getFilePath(srcPath);
    File dst = new File(dstPath);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(srcPath));
    TachyonFile tFile = tachyonClient.getFile(folder);

    // tachyonClient.getFile() catches FileDoesNotExist exceptions and returns null
    if (tFile == null) {
      throw new IOException(folder);
    }

    InStream is = tFile.getInStream(ReadType.NO_CACHE);
    FileOutputStream out = new FileOutputStream(dst);
    byte[] buf = new byte[512];
    int t = is.read(buf);
    while (t != -1) {
      out.write(buf, 0, t);
      t = is.read(buf);
    }
    out.close();
    System.out.println("Copied " + srcPath + " to " + dstPath);
    return 0;
  }

  /**
   * Displays the number of folders and files matching the specified prefix in argv.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int count(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs count <path>");
      return -1;
    }
    String path = argv[1];
    long[] values = countHelper(path);
    String format = "%-25s%-25s%-15s\n";
    System.out.format(format, "File Count", "Folder Count", "Total Bytes");
    System.out.format(format, values[0], values[1], values[2]);
    return 0;
  }

  private long[] countHelper(String path) throws IOException {
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    String folder = Utils.getFilePath(path);
    TachyonFile tFile = tachyonClient.getFile(folder);

    if (tFile.isFile()) {
      return new long[] { 1L, 0L, tFile.length() };
    }

    long[] rtn = new long[] { 0L, 1L, 0L };

    List<ClientFileInfo> files = tachyonClient.listStatus(folder);
    Collections.sort(files);
    for (ClientFileInfo file : files) {
      long[] toAdd = countHelper(file.getPath());
      rtn[0] += toAdd[0];
      rtn[1] += toAdd[1];
      rtn[2] += toAdd[2];
    }
    return rtn;
  }

  /**
   * Displays the file's all blocks info
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int fileinfo(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs fileinfo <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    int fileId = tachyonClient.getFileId(file);
    List<ClientBlockInfo> blocks = tachyonClient.getFileBlocks(fileId);
    System.out.println(file + " with file id " + fileId + " have following blocks: ");
    for (ClientBlockInfo block : blocks) {
      System.out.println(block);
    }
    return 0;
  }

  /**
   * Displays a list of hosts that have the file specified in argv stored.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int location(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs location <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    int fileId = tachyonClient.getFileId(file);
    List<String> hosts = tachyonClient.getFile(fileId).getLocationHosts();
    System.out.println(file + " with file id " + fileId + " are on nodes: ");
    for (String host : hosts) {
      System.out.println(host);
    }
    return 0;
  }

  /**
   * Displays information for all directories and files directly under the path specified in argv.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int ls(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs ls <path>");
      return -1;
    }
    String path = argv[1];
    String folder = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    List<ClientFileInfo> files = tachyonClient.listStatus(folder);
    Collections.sort(files);
    String format = "%-10s%-25s%-15s%-5s\n";
    for (ClientFileInfo file : files) {
      String inMemory = "";
      if (!file.isFolder) {
        if (100 == file.inMemoryPercentage) {
          inMemory = "In Memory";
        } else {
          inMemory = "Not In Memory";
        }
      }
      System.out.format(format, CommonUtils.getSizeFromBytes(file.getLength()),
          CommonUtils.convertMsToDate(file.getCreationTimeMs()), inMemory, file.getPath());
    }
    return 0;
  }

  /**
   * Displays information for all directories and files under the path specified in argv
   * recursively.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int lsr(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs lsr <path>");
      return -1;
    }
    String path = argv[1];
    String folder = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    List<ClientFileInfo> files = tachyonClient.listStatus(folder);
    Collections.sort(files);
    String format = "%-10s%-25s%-15s%-5s\n";
    for (ClientFileInfo file : files) {
      String inMemory = "";
      if (!file.isFolder) {
        if (100 == file.inMemoryPercentage) {
          inMemory = "In Memory";
        } else {
          inMemory = "Not In Memory";
        }
      }
      System.out.format(format, CommonUtils.getSizeFromBytes(file.getLength()),
          CommonUtils.convertMsToDate(file.getCreationTimeMs()), inMemory, file.getPath());
      if (file.isFolder) {
        lsr(new String[] { "lsr", file.getPath() });
      }
    }
    return 0;
  }

  /**
   * Creates a new directory specified by the path in argv, including any parent folders that
   * are required. This method fails if a directory or file with the same path already exists.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int mkdir(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs mkdir <path>");
      return -1;
    }
    String path = argv[1];
    String folder = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    if (tachyonClient.mkdir(folder)) {
      System.out.println("Successfully created directory " + folder);
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Pins the given file or folder (recursively pinning all children if a folder). Pinned files
   * are never evicted from memory.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int pin(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs pin <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    int fileId = tachyonClient.getFileId(file);
    tachyonClient.pinFile(fileId);
    try {
      tachyonClient.pinFile(fileId);
      System.out.println("File '" + file + "' was successfully pinned.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("File '" + file + "' could not be pinned.");
      return -1;
    }
  }

  /**
   * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
   * are never evicted from memory, so this method will allow such files to be evicted.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int unpin(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs unpin <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    int fileId = tachyonClient.getFileId(file);
    try {
      tachyonClient.unpinFile(fileId);
      System.out.println("File '" + file + "' was successfully unpinned.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("File '" + file + "' could not be unpinned.");
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
    System.out.println("       [tail <path>]");
    System.out.println("       [touch <path>]");
    System.out.println("       [mv <src> <dst>]");
    System.out.println("       [copyFromLocal <src> <remoteDst>]");
    System.out.println("       [copyToLocal <src> <localDst>]");
    System.out.println("       [fileinfo <path>]");
    System.out.println("       [location <path>]");
    System.out.println("       [report <path>]");
    System.out.println("       [request <tachyonaddress> <dependencyId>]");
    System.out.println("       [pin <path>]");
    System.out.println("       [unpin <path>]");
  }

  /**
   * Renames a file or directory specified by argv. Will fail if the new path name already
   * exists.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws TException
   */
  public int rename(String argv[]) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs mv <src> <dst>");
      return -1;
    }
    String srcPath = argv[1];
    String dstPath = argv[2];
    String srcMasterAddr = Utils.validatePath(srcPath);
    String dstMasterAddr = Utils.validatePath(dstPath);
    if (!srcMasterAddr.startsWith(dstMasterAddr.substring(0, Constants.HEADER.length()))) {
      throw new IOException("The file system of source and destination must be the same");
    }
    String srcFile = Utils.getFilePath(srcPath);
    String dstFile = Utils.getFilePath(dstPath);
    TachyonFS tachyonClient = TachyonFS.get(srcMasterAddr);
    if (tachyonClient.rename(srcFile, dstFile)) {
      System.out.println("Renamed " + srcFile + " to " + dstFile);
      return 0;
    } else {
      return -1;
    }
  }

  public int report(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs report <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    int fileId = tachyonClient.getFileId(file);
    tachyonClient.reportLostFile(fileId);
    System.out.println(file + " with file id " + fileId + " has reported been report lost.");
    return 0;
  }

  public int request(String argv[]) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs request <tachyonaddress> <dependencyId>");
      return -1;
    }
    String path = argv[1];
    int depId = Integer.parseInt(argv[2]);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    tachyonClient.requestFilesInDependency(depId);
    System.out.println("Dependency with ID " + depId + " has been requested.");
    return 0;
  }

  /**
   * Removes the file or directory specified by argv. Will remove all files and directories in
   * the directory if a directory is specified.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int rm(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs rm <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    if (tachyonClient.delete(file, true)) {
      System.out.println(file + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Method which determines how to handle the user's request, will display usage help to the
   * user if command format is incorrect.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   * @throws TException
   */
  public int run(String argv[]) throws TException {
    if (argv.length == 0) {
      printUsage();
      return -1;
    }

    String cmd = argv[0];
    int exitCode = -1;
    try {
      if (cmd.equals("cat")) {
        exitCode = cat(argv);
      } else if (cmd.equals("count")) {
        exitCode = count(argv);
      } else if (cmd.equals("ls")) {
        exitCode = ls(argv);
      } else if (cmd.equals("lsr")) {
        exitCode = lsr(argv);
      } else if (cmd.equals("mkdir")) {
        exitCode = mkdir(argv);
      } else if (cmd.equals("rm")) {
        exitCode = rm(argv);
      } else if (cmd.equals("tail")) {
        exitCode = tail(argv);
      } else if (cmd.equals("mv")) {
        exitCode = rename(argv);
      } else if (cmd.equals("touch")) {
        exitCode = touch(argv);
      } else if (cmd.equals("copyFromLocal")) {
        exitCode = copyFromLocal(argv);
      } else if (cmd.equals("copyToLocal")) {
        exitCode = copyToLocal(argv);
      } else if (cmd.equals("fileinfo")) {
        exitCode = fileinfo(argv);
      } else if (cmd.equals("location")) {
        exitCode = location(argv);
      } else if (cmd.equals("report")) {
        exitCode = report(argv);
      } else if (cmd.equals("request")) {
        exitCode = request(argv);
      } else if (cmd.equals("pin")) {
        exitCode = pin(argv);
      } else if (cmd.equals("unpin")) {
        exitCode = unpin(argv);
      } else {
        printUsage();
        return -1;
      }
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
    } finally {
    }

    return exitCode;
  }

  /**
   * Prints the file's last 1KB of contents to the console.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.f
   * @throws IOException
   */
  public int tail(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs tail <path>");
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    TachyonFile tFile = tachyonClient.getFile(file);

    if (tFile == null) {
      System.out.println(file + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      byte[] buf = new byte[Constants.KB];
      long bytesToRead = 0L;
      if (tFile.length() > Constants.KB) {
        bytesToRead = Constants.KB;
      } else {
        bytesToRead = tFile.length();
      }
      is.skip(tFile.length() - bytesToRead);
      int read = is.read(buf);
      System.out.write(buf, 0, read);
      return 0;
    } else {
      System.out.println(file + " is not a file.");
      return -1;
    }
  }

  /**
   * Creates a 0 byte file specified by argv.
   * 
   * @param argv
   *          [] Array of arguments given by the user's input from the terminal
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws IOException
   */
  public int touch(String argv[]) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs touch <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonFS tachyonClient = TachyonFS.get(Utils.validatePath(path));
    TachyonFile tFile = tachyonClient.getFile(tachyonClient.createFile(file));
    OutputStream out = tFile.getOutStream(WriteType.THROUGH);
    out.close();
    System.out.println(path + " has been created");
    return 0;
  }
}
