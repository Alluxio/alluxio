package tachyon.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import tachyon.Constants;

/**
 * The recompute command class. Used to execute the recomputation.
 */
public class RecomputeCommand implements Runnable {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final String mCommand;
  private final String mFilePath;

  /**
   * Create a new RecomputeCommand.
   * 
   * @param cmd The command to execute
   * @param filePath The path of the output file, which records the output of the recompute process.
   */
  public RecomputeCommand(String cmd, String filePath) {
    mCommand = cmd;
    mFilePath = filePath;
  }

  @Override
  public void run() {
    try {
      LOG.info("Exec " + mCommand + " output to " + mFilePath);
      Process p = java.lang.Runtime.getRuntime().exec(mCommand);
      String line;
      BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader bre = new BufferedReader(new InputStreamReader(p.getErrorStream()));
      File file = new File(mFilePath);
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      while ((line = bri.readLine()) != null) {
        bw.write(line + "\n");
      }
      bri.close();
      while ((line = bre.readLine()) != null) {
        bw.write(line + "\n");
      }
      bre.close();
      bw.flush();
      bw.close();
      p.waitFor();
      LOG.info("Exec " + mCommand + " output to " + mFilePath + " done.");
    } catch (IOException e) {
      LOG.error(e.getMessage());
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
  }
}
