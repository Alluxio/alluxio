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
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAttributePOptions;
import alluxio.security.authorization.Mode;
import alluxio.util.io.PathUtils;

import com.google.common.base.Joiner;
import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or a directory in the Alluxio filesystem.
 */
@ThreadSafe
@PublicApi
public final class CpCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CpCommand.class);
  private static final String COPY_SUCCEED_MESSAGE = "Copied %s to %s";
  private static final String COPY_FAIL_MESSAGE = "Failed to copy %s to %s";
  private static final int COPY_FROM_LOCAL_BUFFER_SIZE_DEFAULT = 8 * Constants.MB;
  private static final int COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT = 64 * Constants.MB;

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("copy files in subdirectories recursively")
          .build();
  public static final Option THREAD_OPTION =
      Option.builder()
          .longOpt("thread")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("threads")
          .type(Number.class)
          .desc("Number of threads used to copy files in parallel, default value is CPU cores * 2")
          .build();
  public static final Option BUFFER_SIZE_OPTION =
      Option.builder()
          .longOpt("buffersize")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("buffer size")
          .type(Number.class)
          .desc("Read buffer size in bytes, "
              + "default is 8MB when copying from local, "
              + "and 64MB when copying to local")
          .build();
  private static final Option PRESERVE_OPTION =
      Option.builder("p")
          .longOpt("preserve")
          .required(false)
          .desc("Preserve file permission attributes when copying files. "
              + "All ownership, permissions and ACLs will be preserved")
          .build();

  private int mCopyFromLocalBufferSize = COPY_FROM_LOCAL_BUFFER_SIZE_DEFAULT;
  private int mCopyToLocalBufferSize = COPY_TO_LOCAL_BUFFER_SIZE_DEFAULT;
  private int mThread = Runtime.getRuntime().availableProcessors() * 2;
  private boolean mPreservePermissions = false;

  /**
   * A thread pool executor for asynchronous copy.
   *
   * Copy tasks can send messages to an output stream in a thread safe way.
   */
  @ThreadSafe
  private static final class CopyThreadPoolExecutor {
    private static final class CopyException extends Exception {
      public CopyException(AlluxioURI src, AlluxioURI dst, Exception cause) {
        super(String.format(COPY_FAIL_MESSAGE, src, dst), cause);
      }
    }

    private static final Object MESSAGE_DONE = new Object();

    private final ThreadPoolExecutor mPool;
    /**
     * Message queue used by mPrinter.
     * Only supports objects of type String and Exception.
     * String messages will be printed to stdout;
     * Exception messages will be printed to stderr and collected into mExceptions.
     * Other types of messages will be ignored.
     */
    private final BlockingQueue<Object> mMessages;
    private final ConcurrentLinkedQueue<Exception> mExceptions;
    private final PrintStream mStdout;
    private final PrintStream mStderr;
    private final Thread mPrinter;
    private final FileSystem mFileSystem;
    private final AlluxioURI mPath;

    /**
     * Creates a new thread pool with the specified number of threads,
     * specify the output stream for tasks to send messages to, and
     * starts the background thread for printing messages.
     *
     * NOTE: needs to call {@link #shutdown()} to release resources.
     *
     * @param threads number of threads
     * @param stdout the stdout stream for tasks to send messages to
     * @param stderr the stderr stream for tasks to send error messages to
     * @param fileSystem the Alluxio filesystem used to delete path
     * @param path the path to delete on shutdown when it's empty, otherwise can be {@code null}
     */
    public CopyThreadPoolExecutor(int threads, PrintStream stdout, PrintStream stderr,
        FileSystem fileSystem, AlluxioURI path) {
      mPool = new ThreadPoolExecutor(threads, threads,
          1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(threads * 2),
          new ThreadPoolExecutor.CallerRunsPolicy());
      mMessages = new LinkedBlockingQueue<>();
      mExceptions = new ConcurrentLinkedQueue<>();
      mStdout = stdout;
      mStderr = stderr;
      mPrinter = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Object message = mMessages.take();
            if (message == MESSAGE_DONE) {
              break;
            }
            if (message instanceof String) {
              mStdout.println(message);
            } else if (message instanceof CopyException) {
              CopyException e = (CopyException) message;
              mStderr.println(messageAndCause(e));
            } else {
              LOG.error("Unsupported message type " + message.getClass()
                  + " in message queue of copy thread pool");
            }
          } catch (InterruptedException e) {
            break;
          }
        }
      });
      mPrinter.start();
      mFileSystem = fileSystem;
      mPath = path;
    }

    /**
     * Submits a copy task, returns immediately without waiting for completion.
     *
     * @param task the copy task
     */
    public <T> void submit(Callable<T> task) {
      mPool.submit(task);
    }

    /**
     * Sends a message to the pool to indicate that src is copied to dst,
     * the message will be displayed in the stdout stream.
     *
     * @param src the source path
     * @param dst the destination path
     * @throws InterruptedException if interrupted while waiting to send the message
     */
    public void succeed(AlluxioURI src, AlluxioURI dst) throws InterruptedException {
      mMessages.put(String.format(COPY_SUCCEED_MESSAGE, src, dst));
    }

    /**
     * Sends the exception to the pool to indicate that src fails to be copied to dst,
     * the exception will be displayed in the stderr stream.
     *
     * @param src the source path
     * @param dst the destination path
     * @param cause the cause of the failure
     * @throws InterruptedException if interrupted while waiting to send the exception
     */
    public void fail(AlluxioURI src, AlluxioURI dst, Exception cause) throws InterruptedException {
      CopyException exception = new CopyException(src, dst, cause);
      mExceptions.add(exception);
      mMessages.put(exception);
    }

    /**
     * Waits until all asynchronous copy tasks succeed or fail, then shuts down the thread pool,
     * joins the printer thread, and deletes the copy destination in case of error.
     *
     * @throws IOException summarizing all exceptions thrown in the submitted tasks and in shutdown
     */
    public void shutdown() throws IOException {
      mPool.shutdown();
      try {
        mPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Copy thread pool is interrupted in shutdown.", e);
        Thread.currentThread().interrupt();
        mPool.shutdownNow();
      }

      try {
        mMessages.put(MESSAGE_DONE);
        mPrinter.join();
      } catch (InterruptedException e) {
        LOG.warn("Message queue or printer in copy thread pool is interrupted in shutdown.", e);
        Thread.currentThread().interrupt();
        mPrinter.interrupt();
      }

      try {
        if (mPath != null
            && mFileSystem.exists(mPath)
            && mFileSystem.getStatus(mPath).isFolder()
            && mFileSystem.listStatus(mPath).isEmpty()) {
          mFileSystem.delete(mPath);
        }
      } catch (Exception e) {
        mExceptions.add(new IOException("Failed to delete path " + mPath.toString(), e));
      }

      if (!mExceptions.isEmpty()) {
        List<String> errors = new ArrayList<>();
        for (Exception e : mExceptions) {
          LOG.error(stacktrace(e));
          errors.add(messageAndCause(e));
        }
        throw new IOException("ERRORS:\n" + Joiner.on("\n").join(errors));
      }
    }

    private String messageAndCause(Exception e) {
      return e.getMessage() + ": " + e.getCause().getMessage();
    }

    private String stacktrace(Exception e) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(os, true);
      e.printStackTrace(ps);
      ps.close();
      return os.toString();
    }
  }

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public CpCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "cp";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
    if (cl.hasOption(BUFFER_SIZE_OPTION.getLongOpt())) {
      try {
        int bufSize = ((Number) cl.getParsedOptionValue(BUFFER_SIZE_OPTION.getLongOpt()))
            .intValue();
        if (bufSize < 0) {
          throw new InvalidArgumentException(BUFFER_SIZE_OPTION.getLongOpt() + " must be > 0");
        }
        mCopyFromLocalBufferSize = bufSize;
        mCopyToLocalBufferSize = bufSize;
      } catch (ParseException e) {
        throw new InvalidArgumentException("Failed to parse option "
            + BUFFER_SIZE_OPTION.getLongOpt() + " into an integer", e);
      }
    }
    if (cl.hasOption(THREAD_OPTION.getLongOpt())) {
      try {
        mThread = ((Number) cl.getParsedOptionValue(THREAD_OPTION.getLongOpt())).intValue();
        if (mThread <= 0) {
          throw new InvalidArgumentException(THREAD_OPTION.getLongOpt() + " must be > 0");
        }
      } catch (ParseException e) {
        throw new InvalidArgumentException("Failed to parse option " + THREAD_OPTION.getLongOpt()
            + " into an integer", e);
      }
    }
    if (cl.hasOption(PRESERVE_OPTION.getLongOpt())) {
      mPreservePermissions = true;
    } else {
      mPreservePermissions = false;
    }
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION)
        .addOption(THREAD_OPTION)
        .addOption(PRESERVE_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);
    if ((dstPath.getScheme() == null || isAlluxio(dstPath.getScheme()))
        && isFile(srcPath.getScheme())) {
      List<AlluxioURI> srcPaths = new ArrayList<>();
      if (srcPath.containsWildcard()) {
        List<File> srcFiles = FileSystemShellUtils.getFiles(srcPath.getPath());
        if (srcFiles.size() == 0) {
          throw new IOException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcPath));
        }
        for (File srcFile : srcFiles) {
          srcPaths.add(
              new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), srcFile.getPath()));
        }
      } else {
        File src = new File(srcPath.getPath());
        if (src.isDirectory()) {
          File[] files = src.listFiles();
          if (files == null) {
            throw new IOException(String.format("Failed to list files for directory %s", src));
          }
          for (File f : files) {
            srcPaths.add(new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), f.getPath()));
          }
        } else {
          srcPaths.add(srcPath);
        }
      }
      if (srcPaths.size() == 1) {
        copyFromLocalFile(srcPaths.get(0), dstPath);
      } else {
        CopyThreadPoolExecutor pool = new CopyThreadPoolExecutor(mThread, System.out, System.err,
            mFileSystem, mFileSystem.exists(dstPath) ? null : dstPath);
        try {
          createDstDir(dstPath);
          for (AlluxioURI src : srcPaths) {
            AlluxioURI dst = new AlluxioURI(dstPath, new AlluxioURI(src.getName()));
            asyncCopyLocalPath(pool, src, dst);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } finally {
          pool.shutdown();
        }
      }
      System.out.println(String.format(COPY_SUCCEED_MESSAGE, srcPath, dstPath));
    } else if ((srcPath.getScheme() == null || isAlluxio(srcPath.getScheme()))
        && isFile(dstPath.getScheme())) {
      List<AlluxioURI> srcPaths = FileSystemShellUtils.getAlluxioURIs(mFileSystem, srcPath);
      if (srcPaths.size() == 0) {
        throw new IOException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcPath));
      }
      if (srcPath.containsWildcard()) {
        copyWildcardToLocal(srcPaths, dstPath);
      } else {
        copyToLocal(srcPath, dstPath);
      }
    } else if ((srcPath.getScheme() == null || isAlluxio(srcPath.getScheme()))
        && (dstPath.getScheme() == null || isAlluxio(dstPath.getScheme()))) {
      List<AlluxioURI> srcPaths = FileSystemShellUtils.getAlluxioURIs(mFileSystem, srcPath);
      if (srcPaths.size() == 0) {
        throw new FileDoesNotExistException(
            ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcPath.getPath()));
      }
      if (srcPath.containsWildcard()) {
        copyWildcard(srcPaths, dstPath, cl.hasOption(RECURSIVE_OPTION.getOpt()));
      } else {
        copy(srcPath, dstPath, cl.hasOption(RECURSIVE_OPTION.getOpt()));
      }
    } else {
      throw new InvalidPathException(
          "Schemes must be either file or alluxio, and at most one file scheme is allowed.");
    }
    return 0;
  }

  /**
   * Copies a list of files or directories specified by srcPaths to the destination specified by
   * dstPath. This method is used when the original source path contains wildcards.
   *
   * @param srcPaths a list of files or directories in the Alluxio filesystem
   * @param dstPath the destination in the Alluxio filesystem
   * @param recursive indicates whether directories should be copied recursively
   */
  private void copyWildcard(List<AlluxioURI> srcPaths, AlluxioURI dstPath, boolean recursive)
      throws AlluxioException, IOException {
    URIStatus dstStatus = null;
    try {
      dstStatus = mFileSystem.getStatus(dstPath);
    } catch (FileDoesNotExistException e) {
      // if the destination does not exist, it will be created
    }

    if (dstStatus != null && !dstStatus.isFolder()) {
      throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
    }
    if (dstStatus == null) {
      mFileSystem.createDirectory(dstPath);
      System.out.println("Created directory: " + dstPath);
    }
    List<String> errorMessages = new ArrayList<>();
    for (AlluxioURI srcPath : srcPaths) {
      try {
        copy(srcPath, new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(),
            PathUtils.concatPath(dstPath.getPath(), srcPath.getName())), recursive);
      } catch (AlluxioException | IOException e) {
        errorMessages.add(e.getMessage());
      }
    }
    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a file or a directory in the Alluxio filesystem.
   *
   * @param srcPath the source {@link AlluxioURI} (could be a file or a directory)
   * @param dstPath the {@link AlluxioURI} of the destination path in the Alluxio filesystem
   * @param recursive indicates whether directories should be copied recursively
   */
  private void copy(AlluxioURI srcPath, AlluxioURI dstPath, boolean recursive)
      throws AlluxioException, IOException {
    URIStatus srcStatus = mFileSystem.getStatus(srcPath);

    URIStatus dstStatus = null;
    try {
      dstStatus = mFileSystem.getStatus(dstPath);
    } catch (FileDoesNotExistException e) {
      // if the destination does not exist, it will be created
    }

    if (!srcStatus.isFolder()) {
      if (dstStatus != null && dstStatus.isFolder()) {
        dstPath = new AlluxioURI(PathUtils.concatPath(dstPath.getPath(), srcPath.getName()));
      }
      copyFile(srcPath, dstPath);
    } else {
      if (!recursive) {
        throw new IOException(
            srcPath.getPath() + " is a directory, to copy it please use \"cp -R <src> <dst>\"");
      }

      List<URIStatus> statuses;
      statuses = mFileSystem.listStatus(srcPath);

      if (dstStatus != null) {
        if (!dstStatus.isFolder()) {
          throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
        }
        // if copying a directory to an existing directory, the copied directory will become a
        // subdirectory of the destination
        if (srcStatus.isFolder()) {
          dstPath = new AlluxioURI(PathUtils.concatPath(dstPath.getPath(), srcPath.getName()));
          mFileSystem.createDirectory(dstPath);
          System.out.println("Created directory: " + dstPath);
        }
      }

      if (dstStatus == null) {
        mFileSystem.createDirectory(dstPath);
        System.out.println("Created directory: " + dstPath);
      }

      preserveAttributes(srcPath, dstPath);
      List<String> errorMessages = new ArrayList<>();
      for (URIStatus status : statuses) {
        try {
          copy(new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), status.getPath()),
              new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(),
                  PathUtils.concatPath(dstPath.getPath(), status.getName())), recursive);
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    }
  }

  /**
   * Copies a file in the Alluxio filesystem.
   *
   * @param srcPath the source {@link AlluxioURI} (has to be a file)
   * @param dstPath the destination path in the Alluxio filesystem
   */
  private void copyFile(AlluxioURI srcPath, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    try (FileInStream is = mFileSystem.openFile(srcPath);
         FileOutStream os = mFileSystem.createFile(dstPath)) {
      try {
        IOUtils.copyLarge(is, os, new byte[8 * Constants.MB]);
      } catch (Exception e) {
        os.cancel();
        // clean up the incomplete file
        mFileSystem.delete(dstPath, DeletePOptions.newBuilder().setUnchecked(true).build());
        throw e;
      }
      System.out.println(String.format(COPY_SUCCEED_MESSAGE, srcPath, dstPath));
    }
    preserveAttributes(srcPath, dstPath);
  }

  /**
   * Preserves attributes from the source file to the target file.
   *
   * @param srcPath the source path
   * @param dstPath the destination path in the Alluxio filesystem
   */
  private void preserveAttributes(AlluxioURI srcPath, AlluxioURI dstPath)
      throws IOException, AlluxioException {
    if (mPreservePermissions) {
      URIStatus srcStatus = mFileSystem.getStatus(srcPath);
      mFileSystem.setAttribute(dstPath, SetAttributePOptions.newBuilder()
          .setOwner(srcStatus.getOwner())
          .setGroup(srcStatus.getGroup())
          .setMode(new Mode((short) srcStatus.getMode()).toProto())
          .build());
      mFileSystem.setAcl(dstPath, SetAclAction.REPLACE, srcStatus.getAcl().getEntries());
    }
  }

  /**
   * Creates a directory in the Alluxio filesystem space. It will not throw any exception if the
   * destination directory already exists.
   *
   * @param dstPath the {@link AlluxioURI} of the destination directory which will be created
   */
  private void createDstDir(AlluxioURI dstPath) throws AlluxioException, IOException {
    try {
      mFileSystem.createDirectory(dstPath);
    } catch (FileAlreadyExistsException e) {
      // it's fine if the directory already exists
    }

    URIStatus dstStatus = mFileSystem.getStatus(dstPath);
    if (!dstStatus.isFolder()) {
      throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
    }
  }

  private void copyFromLocalFile(AlluxioURI srcPath, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    File src = new File(srcPath.getPath());
    if (src.isDirectory()) {
      throw new IOException("Source " + src.getAbsolutePath() + " is not a file.");
    }
    // If the dstPath is a directory, then it should be updated to be the path of the file where
    // src will be copied to.
    if (mFileSystem.exists(dstPath) && mFileSystem.getStatus(dstPath).isFolder()) {
      dstPath = dstPath.join(src.getName());
    }

    FileOutStream os = null;
    try (Closer closer = Closer.create()) {
      os = closer.register(mFileSystem.createFile(dstPath));
      FileInputStream in = closer.register(new FileInputStream(src));
      FileChannel channel = closer.register(in.getChannel());
      ByteBuffer buf = ByteBuffer.allocate(mCopyFromLocalBufferSize);
      while (channel.read(buf) != -1) {
        buf.flip();
        os.write(buf.array(), 0, buf.limit());
      }
    } catch (Exception e) {
      // Close the out stream and delete the file, so we don't have an incomplete file lying
      // around.
      if (os != null) {
        os.cancel();
        if (mFileSystem.exists(dstPath)) {
          mFileSystem.delete(dstPath);
        }
      }
      throw e;
    }
  }

  /**
   * Asynchronously copies a file or directory specified by srcPath from the local filesystem to
   * dstPath in the Alluxio filesystem space, assuming dstPath does not exist.
   *
   * @param srcPath the {@link AlluxioURI} of the source file in the local filesystem
   * @param dstPath the {@link AlluxioURI} of the destination
   * @throws InterruptedException when failed to send messages to the pool
   */
  private void asyncCopyLocalPath(CopyThreadPoolExecutor pool, AlluxioURI srcPath,
      AlluxioURI dstPath) throws InterruptedException {
    File src = new File(srcPath.getPath());
    if (!src.isDirectory()) {
      pool.submit(() -> {
        try {
          copyFromLocalFile(srcPath, dstPath);
          pool.succeed(srcPath, dstPath);
        } catch (Exception e) {
          pool.fail(srcPath, dstPath, e);
        }
        return null;
      });
    } else {
      try {
        mFileSystem.createDirectory(dstPath);
      } catch (Exception e) {
        pool.fail(srcPath, dstPath, e);
        return;
      }
      File[] fileList = src.listFiles();
      if (fileList == null) {
        pool.fail(srcPath, dstPath,
            new IOException(String.format("Failed to list directory %s.", src)));
        return;
      }
      for (File srcFile : fileList) {
        AlluxioURI newURI = new AlluxioURI(dstPath, new AlluxioURI(srcFile.getName()));
        asyncCopyLocalPath(pool,
              new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), srcFile.getPath()),
              newURI);
      }
    }
  }

  /**
   * Copies a list of files or directories specified by srcPaths from the Alluxio filesystem to
   * dstPath in the local filesystem. This method is used when the input path contains wildcards.
   *
   * @param srcPaths the list of files in the Alluxio filesystem
   * @param dstPath the {@link AlluxioURI} of the destination directory in the local filesystem
   */
  private void copyWildcardToLocal(List<AlluxioURI> srcPaths, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    File dstFile = new File(dstPath.getPath());
    if (dstFile.exists() && !dstFile.isDirectory()) {
      throw new InvalidPathException(ExceptionMessage.DESTINATION_CANNOT_BE_FILE.getMessage());
    }
    if (!dstFile.exists()) {
      if (!dstFile.mkdirs()) {
        throw new IOException("Fail to create directory: " + dstPath);
      } else {
        System.out.println("Create directory: " + dstPath);
      }
    }
    List<String> errorMessages = new ArrayList<>();
    for (AlluxioURI srcPath : srcPaths) {
      try {
        File dstSubFile = new File(dstFile.getAbsoluteFile(), srcPath.getName());
        copyToLocal(srcPath,
            new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(), dstSubFile.getPath()));
      } catch (IOException e) {
        errorMessages.add(e.getMessage());
      }
    }
    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  /**
   * Copies a file or a directory from the Alluxio filesystem to the local filesystem.
   *
   * @param srcPath the source {@link AlluxioURI} (could be a file or a directory)
   * @param dstPath the {@link AlluxioURI} of the destination in the local filesystem
   */
  private void copyToLocal(AlluxioURI srcPath, AlluxioURI dstPath) throws AlluxioException,
      IOException {
    URIStatus srcStatus = mFileSystem.getStatus(srcPath);
    File dstFile = new File(dstPath.getPath());
    if (srcStatus.isFolder()) {
      // make a local directory
      if (!dstFile.exists()) {
        if (!dstFile.mkdirs()) {
          throw new IOException("mkdir failure for directory: " + dstPath);
        } else {
          System.out.println("Create directory: " + dstPath);
        }
      }

      List<URIStatus> statuses;
      try {
        statuses = mFileSystem.listStatus(srcPath);
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage());
      }

      List<String> errorMessages = new ArrayList<>();
      for (URIStatus status : statuses) {
        try {
          File subDstFile = new File(dstFile.getAbsolutePath(), status.getName());
          copyToLocal(
              new AlluxioURI(srcPath.getScheme(), srcPath.getAuthority(), status.getPath()),
              new AlluxioURI(dstPath.getScheme(), dstPath.getAuthority(), subDstFile.getPath()));
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } else {
      copyFileToLocal(srcPath, dstPath);
    }
  }

  /**
   * Copies a file specified by argv from the filesystem to the local filesystem. This is the
   * utility function.
   *
   * @param srcPath The source {@link AlluxioURI} (has to be a file)
   * @param dstPath The {@link AlluxioURI} of the destination in the local filesystem
   */
  private void copyFileToLocal(AlluxioURI srcPath, AlluxioURI dstPath)
      throws AlluxioException, IOException {
    File dstFile = new File(dstPath.getPath());
    String randomSuffix =
        String.format(".%s_copyToLocal_", RandomStringUtils.randomAlphanumeric(8));
    File outputFile;
    if (dstFile.isDirectory()) {
      outputFile = new File(PathUtils.concatPath(dstFile.getAbsolutePath(), srcPath.getName()));
    } else {
      outputFile = dstFile;
    }
    File tmpDst = new File(outputFile.getPath() + randomSuffix);

    try (Closer closer = Closer.create()) {
      FileInStream is = closer.register(mFileSystem.openFile(srcPath));
      FileOutputStream out = closer.register(new FileOutputStream(tmpDst));
      byte[] buf = new byte[mCopyToLocalBufferSize];
      int t = is.read(buf);
      while (t != -1) {
        out.write(buf, 0, t);
        t = is.read(buf);
      }
      if (!tmpDst.renameTo(outputFile)) {
        throw new IOException(
            "Failed to rename " + tmpDst.getPath() + " to destination " + outputFile.getPath());
      }
      System.out.println("Copied " + srcPath + " to " + "file://" + outputFile.getPath());
    } finally {
      tmpDst.delete();
    }
  }

  @Override
  public String getUsage() {
    return "cp "
        + "[-R] "
        + "[--buffersize <bytes>] "
        + "<src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory in the Alluxio filesystem or between local filesystem "
        + "and Alluxio filesystem. The -R flag is needed to copy directories in the Alluxio "
        + "filesystem. Local Path with schema \"file\".";
  }

  private static boolean isAlluxio(String scheme) {
    return Constants.SCHEME.equals(scheme);
  }

  private static boolean isFile(String scheme) {
    return "file".equals(scheme);
  }
}
