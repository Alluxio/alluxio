/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.WorkerNetAddress;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies the specified file specified by "source path" to the path specified by "remote path".
 * This command will fail if "remote path" already exists.
 */
@ThreadSafe
@PublicApi
public final class ConsistentHashCommand extends AbstractFileSystemCommand {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashCommand.class);

  private final String mFolderName = "/consistent-hash-check-data_ALLUXIO";

  private final int mFileNum = 1000;

  private SimpleDateFormat mSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");

  public static final Option CREATE_CHECK_FILE =
      Option.builder()
          .longOpt("create")
          .required(false)
          .hasArg(false)
          .desc("Generate check file.")
          .build();

  public static final Option COMPARE_CHECK_FILES =
      Option.builder()
          .longOpt("compare")
          .required(false)
          .hasArg(false)
          .desc("Compare check files to see if the hash ring has changed "
              + "and if data lost.")
          .build();

  public static final Option CLEAN_CHECK_DATA =
      Option.builder()
          .longOpt("clean")
          .required(false)
          .hasArg(false)
          .desc("Clean all check data.")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public ConsistentHashCommand(FileSystemContext fsContext) {
    super(fsContext);
    // The copyFromLocal command needs its own filesystem context because we overwrite the
    // block location policy configuration.
    // The original one can't be closed because it may still be in-use within the same shell.
    InstancedConfiguration conf = new InstancedConfiguration(
        fsContext.getClusterConf().copyProperties());

    FileSystemContext updatedCtx = FileSystemContext.sFileSystemContextFactory.create(conf);
    mFsContext = updatedCtx;
    mFileSystem = FileSystem.Factory.create(updatedCtx);
  }

  /**
   * Clean all the check data.
   * @throws IOException
   * @throws AlluxioException
   */
  public void cleanCheckData() throws IOException, AlluxioException {
    AlluxioURI folder = new AlluxioURI(mFolderName);
    for (int i = 0; i < mFileNum; i++) {
      System.out.println("Progress: " + (i + 1) + "/" + mFileNum);
      String fileName = "file" + i;
      AlluxioURI file = new AlluxioURI(folder, new AlluxioURI(fileName));
      if (mFileSystem.exists(file)) {
        mFileSystem.delete(file);
      }
    }
    if (mFileSystem.exists(folder)) {
      mFileSystem.delete(folder);
    }
    System.out.println("Check data has been cleaned successfully.");
  }

  /**
   * Create the check file.
   * @throws IOException
   * @throws AlluxioException
   */
  public void createCheckFile() throws IOException, AlluxioException {
    // Step 1. create folder
    String folderName = "/consistent-hash-check-data_ALLUXIO";
    AlluxioURI folder = new AlluxioURI(folderName);
    if (!mFileSystem.exists(folder)) {
      mFileSystem.createDirectory(folder);
    }

    // Step 2. generate 1000 files and put them into the folder
    Set<FileLocation> fileLocationSet = new HashSet<>();
    for (int i = 0; i < mFileNum; i++) {
      System.out.println("Progress: " + (i + 1) + "/" + mFileNum);
      String fileName = "file" + i;
      AlluxioURI file = new AlluxioURI(folder, new AlluxioURI(fileName));

      boolean hasCachedFile = false;
      if (!mFileSystem.exists(file)) {
        writeFile(file);
        cacheFile(file);
        hasCachedFile = true;
      }

      if (mFileSystem.getDoraCacheFileSystem() != null) {
        DoraCacheFileSystem doraCacheFileSystem = mFileSystem.getDoraCacheFileSystem();
        AlluxioURI ufsFullPath = doraCacheFileSystem.convertToUfsPath(file);
        String fileUfsFullName = ufsFullPath.toString();
        boolean dataOnPreferredWorker = false;
        Set<String> workersThatHaveDataSet = new HashSet<>();

        WorkerNetAddress preferredWorker = doraCacheFileSystem.getWorkerNetAddress(file);
        Map<String, List<WorkerNetAddress>> fileOnWorkersMap = checkFileLocation(file);

        if (fileOnWorkersMap != null && fileOnWorkersMap.size() > 0) {
          Optional<String> fileUfsFullNameOpt = fileOnWorkersMap.keySet().stream().findFirst();
          if (fileUfsFullNameOpt.isPresent()) {
            List<WorkerNetAddress> workersThatHaveDataList = fileOnWorkersMap.get(fileUfsFullName);
            if (workersThatHaveDataList != null && !workersThatHaveDataList.isEmpty()) {
              dataOnPreferredWorker = workersThatHaveDataList.contains(preferredWorker);
              workersThatHaveDataSet = workersThatHaveDataList.stream()
                  .map(workerNetAddress -> workerNetAddress.getHost()).collect(Collectors.toSet());
            }
          }
        }

        FileLocation fileLocation = new FileLocation(
            fileUfsFullName,
            preferredWorker.getHost(),
            dataOnPreferredWorker,
            workersThatHaveDataSet);
        fileLocationSet.add(fileLocation);
      }

      if (hasCachedFile == false) {
        cacheFile(file);
      }
    }

    // Step 3. convert to JSON and persist to UFS
    Gson gson = new Gson();
    String json = gson.toJson(fileLocationSet);
    String persistFileName = "/consistent-hash-check-"
        + mSimpleDateFormat.format(new Date()) + ".json";
    writeFile(new AlluxioURI(persistFileName), json.getBytes());

    System.out.println("Check file " + persistFileName + "  is generated successfully.");
  }

  private Map<String, List<WorkerNetAddress>> checkFileLocation(AlluxioURI file)
      throws IOException {
    if (mFileSystem.getDoraCacheFileSystem() != null) {
      DoraCacheFileSystem doraCacheFileSystem = mFileSystem.getDoraCacheFileSystem();
      Map<String, List<WorkerNetAddress>> pathLocations =
          doraCacheFileSystem.checkFileLocation(file);
      return pathLocations;
    } else {
      throw new RuntimeException("Only DORA architecture can use this command. ");
    }
  }

  private void cacheFile(AlluxioURI file) throws IOException, AlluxioException {
    byte[] buf = new byte[Constants.MB];
    try (FileInStream is = mFileSystem.openFile(file)) {
      int read = is.read(buf);
      while (read != -1) {
        read = is.read(buf);
      }
    }
  }

  private void writeFile(AlluxioURI file) throws IOException, AlluxioException {
    try (FileOutStream outStream = mFileSystem.createFile(file)) {
      byte[] bytes = new byte[1];
      bytes[0] = 1;
      outStream.write(bytes);
    }
  }

  private void writeFile(AlluxioURI file, byte[] data) throws IOException, AlluxioException {
    BufferedInputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(data));
    try (FileOutStream outStream = mFileSystem.createFile(file)) {
      byte[] buffer = new byte[Constants.MB];
      int bytesRead;
      do {
        bytesRead = inputStream.read(buffer);
        if (bytesRead != -1) {
          outStream.write(buffer, 0, bytesRead);
        }
      } while (bytesRead != -1);
    }
  }

  private CheckResult compareCheckFile(String checkFile, String anotherCheckFile)
      throws IOException, AlluxioException {
    AlluxioURI checkFileUri = new AlluxioURI(checkFile);
    AlluxioURI anotherCheckFileUri = new AlluxioURI(anotherCheckFile);
    Set<FileLocation> fileLocationSet = loadCheckFile(checkFileUri);
    Set<FileLocation> anotherFileLocationSet = loadCheckFile(anotherCheckFileUri);

    Map<String, FileLocation> fileLocationMap = new HashMap<>();
    for (FileLocation fileLocation : fileLocationSet) {
      fileLocationMap.put(fileLocation.getFileName(), fileLocation);
    }
    Map<String, FileLocation> anotherFileLocationMap = new HashMap<>();
    for (FileLocation fileLocation : anotherFileLocationSet) {
      anotherFileLocationMap.put(fileLocation.getFileName(), fileLocation);
    }

    boolean isHashRingChanged = false;
    boolean isDataLost = false;
    for (Map.Entry<String, FileLocation> entry : fileLocationMap.entrySet()) {
      String fileName = entry.getKey();
      FileLocation fileLocation = entry.getValue();
      FileLocation anotherFileLocation = anotherFileLocationMap.get(fileName);

      if (anotherFileLocation == null
          || (!fileLocation.getPreferredWorker()
          .equals(anotherFileLocation.getPreferredWorker()))) {
        isHashRingChanged = true;
      }

      if (anotherFileLocation == null
          || (fileLocation.isDataOnPreferredWorker()
          && !anotherFileLocation.isDataOnPreferredWorker())) {
        isDataLost = true;
      }
    }
    CheckResult checkResult = new CheckResult(isHashRingChanged, isDataLost);
    Gson gson = new Gson();
    System.out.println(gson.toJson(checkResult));
    return checkResult;
  }

  private Set<FileLocation> loadCheckFile(AlluxioURI checkFileUri)
      throws IOException, AlluxioException {
    StringBuffer stringBuffer = new StringBuffer();
    byte[] buf = new byte[Constants.MB];
    try (FileInStream is = mFileSystem.openFile(checkFileUri)) {
      int read = is.read(buf);
      while (read != -1) {
        stringBuffer.append(new String(buf, 0, read));
        read = is.read(buf);
      }
    }
    String json = stringBuffer.toString();
    Gson gson = new Gson();
    Type type = new TypeToken<HashSet<FileLocation>>(){}.getType();
    Set<FileLocation> fileLocationSet = gson.fromJson(json, type);
    return fileLocationSet;
  }

  @Override
  public void close() throws IOException {
    // Close updated {@link FileSystem} instance that is created for internal cp command.
    // This will close the {@link FileSystemContext} associated with it.
    mFileSystem.close();
  }

  @Override
  public String getCommandName() {
    return "consistent-hash";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ConsistentHashCommand.CREATE_CHECK_FILE)
        .addOption(ConsistentHashCommand.COMPARE_CHECK_FILES)
        .addOption(ConsistentHashCommand.CLEAN_CHECK_DATA);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    Option[] options = cl.getOptions();
    switch (options[0].getLongOpt()) {
      case "create":
        createCheckFile();
        break;
      case "compare":
        String checkFilePath = args[0];
        String anotherCheckFilePath = args[1];
        compareCheckFile(checkFilePath, anotherCheckFilePath);
        break;
      case "clean":
        cleanCheckData();
        break;
      default:
        System.out.println(getUsage());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "consistent-hash "
        + "[--create] "
        + "[--compare <1stCheckFilePath> <2ndCheckFilePath>] "
        + "[--clean] ";
  }

  @Override
  public String getDescription() {
    return "This command is for checking whether the consistent hash ring is changed or not. "
        + "The command will generates 1000 files and caches them in Alluxio Workers. And then "
        + "create a check file which records the location of each file. Next time we can execute "
        + "this command again, and check if the check files are the same. If they are different, "
        + "it means that the consistent hash ring has changed.";
  }

  class CheckResult {

    private boolean mHashRingChanged;

    private boolean mDataLost;

    public CheckResult(boolean hashRingChanged, boolean dataLost) {
      mHashRingChanged = hashRingChanged;
      mDataLost = dataLost;
    }

    public boolean isHashRingChanged() {
      return mHashRingChanged;
    }

    public boolean isDataLost() {
      return mDataLost;
    }
  }
}
