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

package tachyon.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A base class for running a Unix command */
public abstract class Shell {
  public static final Logger LOG = LoggerFactory.getLogger(Shell.class);
  /** a Unix command to get the current user's name */
  public static final String USER_NAME_COMMAND = "whoami";
  /** a Unix command to set permission */
  public static final String SET_PERMISSION_COMMAND = "chmod";
  /** a Unix command to set owner */
  public static final String SET_OWNER_COMMAND = "chown";
  /** a Unix command to set the change user's groups list */
  public static final String SET_GROUP_COMMAND = "chgrp";
  /** a Unix command to get the current user's groups list */
  public static String[] getGroupsCommand() {
    return new String[]{"bash", "-c", "groups"};
  }

  /** a Unix command to get a given user's groups list */
  public static String[] getGroupsForUserCommand(final String user) {
    return new String [] {"bash", "-c", "id -gn " + user
                                     + "&& id -Gn " + user};
  }

  /** Return a command to get permission information. */
  public static String[] getGetPermissionCommand() {
    return new String[] { "/bin/ls", "-ld" };
  }

  /** Return a command to set permission */
  public static String[] getSetPermissionCommand(String perm, File file, boolean recursive) {
    if (recursive) {
      return new String[] { "chmod", "-R", perm, file.getAbsolutePath()};
    } else {
      return new String[] { "chmod", perm, file.getAbsolutePath()};
    }
  }

  /** Return a command to set owner */
  public static String[] getSetOwnerCommand(String owner, File file) {
    return new String[] { "chown", owner, file.getAbsolutePath()};
  }

  /** Return a regular expression string that match environment variables */
  public static String getEnvironmentVariableRegex() {
    return "\\$([A-Za-z_][A-Za-z0-9_]*)";
  }

  /**
   * Returns a File referencing a script with the given basename, inside the
   * given parent directory.  The file extension is inferred by platform: ".cmd"
   * on Windows, or ".sh" otherwise.
   * 
   * @param parent File parent directory
   * @param basename String script file basename
   * @return File referencing the script in the directory
   */
  public static File appendScriptExtension(File parent, String basename) {
    return new File(parent, appendScriptExtension(basename));
  }

  /**
   * Returns a script file name with the given basename.
   * 
   * @param basename String script file basename
   * @return String script file name
   */
  public static String appendScriptExtension(String basename) {
    return basename + ".sh";
  }

  /**
   * Returns a command to run the given script.  The script interpreter is
   * inferred by platform: cmd on Windows or bash otherwise.
   * 
   * @param script File script to run
   * @return String[] command to run the script
   */
  public static String[] getRunScriptCommand(File script) {
    String absolutePath = script.getAbsolutePath();
    return new String[] { "/bin/bash", absolutePath };
  }

  /**Time after which the executing script would be timedout*/
  protected long mTimeOutInterval = 0L;
  /** If or not script timed out*/
  private AtomicBoolean mTimedOut;

  /** number of nano seconds in 1 millisecond */
  public static final long NANOSECONDS_PER_MILLISECOND = 1000000;
  /** Token separator regex used to parse Shell tool outputs */
  public static final String TOKEN_SEPARATOR_REGEX = "[ \t\n\r\f]";

  private long mInterval;   // refresh interval in msec
  private long mLastTime;   // last time the command was performed
  private boolean mRedirectErrorStream; // merge stdout and stderr
  private Map<String, String> mEnvironment; // env for the command execution
  private File mDir;
  private Process mProcess; // sub process used to execute the command
  private int mExitCode;

  /**If or not script finished executing*/
  private volatile AtomicBoolean mCompleted;

  public Shell() {
    this(0L);
  }

  public Shell(long interval) {
    this(interval, false);
  }

  /**
   * @param interval the minimum duration to wait before re-executing the
   *        command.
   */
  public Shell(long interval, boolean redirectErrorStream) {
    mInterval = interval;
    mLastTime = (interval < 0) ? 0 : -interval;
    mRedirectErrorStream = redirectErrorStream;
  }

  /** set the environment for the command
   * @param env Mapping of environment variables
   */
  protected void setEnvironment(Map<String, String> env) {
    mEnvironment = env;
  }

  /** set the working directory
   * @param dir The directory where the command would be executed
   */
  protected void setWorkingDirectory(File dir) {
    mDir = dir;
  }

  /** check to see if a command needs to be executed and execute if needed */
  protected void run() throws IOException {
    if (mLastTime + mInterval > monotonicNow()) {
      return;
    }
    mExitCode = 0; // reset for next run
    runCommand();
  }

  /** Run a command */
  private void runCommand() throws IOException {
    ProcessBuilder builder = new ProcessBuilder(getExecString());
    Timer timeOutTimer = null;
    ShellTimeoutTimerTask timeoutTimerTask = null;
    mTimedOut = new AtomicBoolean(false);
    mCompleted = new AtomicBoolean(false);

    if (mEnvironment != null) {
      builder.environment().putAll(mEnvironment);
    }
    if (mDir != null) {
      builder.directory(mDir);
    }

    builder.redirectErrorStream(mRedirectErrorStream);

    mProcess = builder.start();

    if (mTimeOutInterval > 0) {
      timeOutTimer = new Timer("Shell command timeout");
      timeoutTimerTask = new ShellTimeoutTimerTask(this);
      //One time scheduling.
      timeOutTimer.schedule(timeoutTimerTask, mTimeOutInterval);
    }
    final BufferedReader errReader = new BufferedReader(
        new InputStreamReader(mProcess.getErrorStream(), Charset.defaultCharset()));
    BufferedReader inReader = new BufferedReader(
        new InputStreamReader(mProcess.getInputStream(), Charset.defaultCharset()));
    final StringBuffer errMsg = new StringBuffer();

    // read error and input streams as this would free up the buffers
    // free the error stream buffer
    Thread errThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = errReader.readLine();
          while ((line != null) && !isInterrupted()) {
            errMsg.append(line);
            errMsg.append(System.getProperty("line.separator"));
            line = errReader.readLine();
          }
        } catch (IOException ioe) {
          LOG.warn("Error reading the error stream", ioe);
        }
      }
    };
    try {
      errThread.start();
    } catch (OutOfMemoryError oe) {
      LOG.error("Caught " + oe + ". One possible reason is that ulimit"
          + " setting of 'max user processes' is too low. If so, do"
          + " 'ulimit -u <largerNum>' and try again.");
      throw oe;
    }
    try {
      parseExecResult(inReader); // parse the output
      // clear the input stream buffer
      String line = inReader.readLine();
      while (line != null) {
        line = inReader.readLine();
      }
      // wait for the process to finish and check the exit code
      mExitCode  = mProcess.waitFor();
      // make sure that the error thread exits
      joinThread(errThread);
      mCompleted.set(true);
      //the timeout thread handling
      //taken care in finally block
      if (mExitCode != 0) {
        throw new ExitCodeException(mExitCode, errMsg.toString());
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie.toString());
    } finally {
      if (timeOutTimer != null) {
        timeOutTimer.cancel();
      }
      // close the input stream
      try {
        // JDK 7 tries to automatically drain the input streams for us
        // when the process exits, but since close is not synchronized,
        // it creates a race if we close the stream first and the same
        // fd is recycled.  the stream draining thread will attempt to
        // drain that fd!!  it may block, OOM, or cause bizarre behavior
        // see: https://bugs.openjdk.java.net/browse/JDK-8024521
        //      issue is fixed in build 7u60
        InputStream stdout = mProcess.getInputStream();
        synchronized (stdout) {
          inReader.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      if (!mCompleted.get()) {
        errThread.interrupt();
        joinThread(errThread);
      }
      try {
        InputStream stderr = mProcess.getErrorStream();
        synchronized (stderr) {
          errReader.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Error while closing the error stream", ioe);
      }
      mProcess.destroy();
      mLastTime = monotonicNow();
    }
  }

  private static void joinThread(Thread t) {
    while (t.isAlive()) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Interrupted while joining on: " + t, ie);
        }
        t.interrupt(); // propagate interrupt
      }
    }
  }

  private static long monotonicNow() {
    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }

  /** return an array containing the command name & its parameters */
  protected abstract String[] getExecString();

  /** Parse the execution result */
  protected abstract void parseExecResult(BufferedReader lines)
  throws IOException;

  /**
   * Get the environment variable
   */
  public String getEnvironment(String env) {
    return mEnvironment.get(env);
  }

  /** get the current sub-process executing the given command
   * @return process executing the command
   */
  public Process getProcess() {
    return mProcess;
  }

  /** get the exit code
   * @return the exit code of the process
   */
  public int getExitCode() {
    return mExitCode;
  }

  /**
   * This is an IOException with exit code added.
   */
  public static class ExitCodeException extends IOException {
    private final int mExitCode;

    public ExitCodeException(int exitCode, String message) {
      super(message);
      mExitCode = exitCode;
    }

    public int getExitCode() {
      return mExitCode;
    }

    @Override
    public String toString() {
      final StringBuilder sb =
          new StringBuilder("ExitCodeException ");
      sb.append("exitCode=").append(mExitCode)
        .append(": ");
      sb.append(super.getMessage());
      return sb.toString();
    }
  }

  public interface CommandExecutor {

    void execute() throws IOException;

    int getExitCode() throws IOException;

    String getOutput() throws IOException;

    void close();

  }

  /**
   * A simple shell command executor.
   * 
   * <code>ShellCommandExecutor</code>should be used in cases where the output
   * of the command needs no explicit parsing and where the command, working
   * directory and the environment remains unchanged. The output of the command
   * is stored as-is and is expected to be small.
   */
  public static class ShellCommandExecutor extends Shell
      implements CommandExecutor {

    private String[] mCommand;
    private StringBuffer mOutput;


    public ShellCommandExecutor(String[] execString) {
      this(execString, null);
    }

    public ShellCommandExecutor(String[] execString, File dir) {
      this(execString, dir, null);
    }

    public ShellCommandExecutor(String[] execString, File dir,
                                 Map<String, String> env) {
      this(execString, dir, env , 0L);
    }

    /**
     * Create a new instance of the ShellCommandExecutor to execute a command.
     * 
     * @param execString The command to execute with arguments
     * @param dir If not-null, specifies the directory which should be set
     *            as the current working directory for the command.
     *            If null, the current working directory is not modified.
     * @param env If not-null, environment of the command will include the
     *            key-value pairs specified in the map. If null, the current
     *            environment is not modified.
     * @param timeout Specifies the time in milliseconds, after which the
     *                command will be killed and the status marked as timedout.
     *                If 0, the command will not be timed out.
     */
    public ShellCommandExecutor(String[] execString, File dir,
        Map<String, String> env, long timeout) {
      mCommand = execString.clone();
      if (dir != null) {
        setWorkingDirectory(dir);
      }
      if (env != null) {
        setEnvironment(env);
      }
      mTimeOutInterval = timeout;
    }


    /** Execute the shell command. */
    public void execute() throws IOException {
      this.run();
    }

    @Override
    public String[] getExecString() {
      return mCommand;
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      mOutput = new StringBuffer();
      char[] buf = new char[512];
      int nRead;
      while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
        mOutput.append(buf, 0, nRead);
      }
    }

    /** Get the output of the shell command.*/
    public String getOutput() {
      return (mOutput == null) ? "" : mOutput.toString();
    }

    /**
     * Returns the commands of this instance.
     * Arguments with spaces in are presented with quotes round; other
     * arguments are presented raw
     * 
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      String[] args = getExecString();
      for (String s : args) {
        if (s.indexOf(' ') >= 0) {
          builder.append('"').append(s).append('"');
        } else {
          builder.append(s);
        }
        builder.append(' ');
      }
      return builder.toString();
    }

    @Override
    public void close() {
    }
  }

  /**
   * To check if the passed script to shell command executor timed out or
   * not.
   * 
   * @return if the script timed out.
   */
  public boolean isTimedOut() {
    return mTimedOut.get();
  }

  /**
   * Set if the command has timed out.
   * 
   */
  private void setTimedOut() {
    this.mTimedOut.set(true);
  }

  /**
   * Static method to execute a shell command.
   * Covers most of the simple cases without requiring the user to implement
   * the <code>Shell</code> interface.
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static String execCommand(String ... cmd) throws IOException {
    return execCommand(null, cmd, 0L);
  }

  /**
   * Static method to execute a shell command.
   * Covers most of the simple cases without requiring the user to implement
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @param timeout time in milliseconds after which script should be marked timeout
   * @return the output of the executed command.o
   */

  public static String execCommand(Map<String, String> env, String[] cmd,
      long timeout) throws IOException {
    ShellCommandExecutor exec = new ShellCommandExecutor(cmd, null, env,
                                                          timeout);
    exec.execute();
    return exec.getOutput();
  }

  /**
   * Static method to execute a shell command.
   * Covers most of the simple cases without requiring the user to implement
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static String execCommand(Map<String,String> env, String ... cmd)
  throws IOException {
    return execCommand(env, cmd, 0L);
  }

  /**
   * Timer which is used to timeout scripts spawned off by shell.
   */
  private static class ShellTimeoutTimerTask extends TimerTask {

    private Shell mShell;

    public ShellTimeoutTimerTask(Shell shell) {
      mShell = shell;
    }

    @Override
    public void run() {
      Process p = mShell.getProcess();
      try {
        p.exitValue();
      } catch (Exception e) {
        //Process has not terminated.
        //So check if it has completed
        //if not just destroy it.
        if (p != null && !mShell.mCompleted.get()) {
          mShell.setTimedOut();
          p.destroy();
        }
      }
    }
  }
}
