package tachyon.examples;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Version;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

public class ReadTest {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static TachyonFS sTachyonClient;
  private static String sOperation = null;
  private static long sBytes = 0;
  private static boolean sCheckData = false;
  private static String[] sFiles = null;

  public static void main(String[] args) throws IOException {
    if (args.length < 4) {
      System.out
          .println("java -cp target/tachyon-"
              + Version.VERSION
              + "-jar-with-dependencies.jar "
              + "tachyon.examples.ReadTest <TachyonMasterAddress>"
              + " <Operation> <FileList_CommaSeparated> <FileBytes> <Optional_CheckData>");
      System.exit(-1);
    }
    sTachyonClient = TachyonFS.get(args[0]);
    sOperation = args[1];
    sFiles = args[2].split(",");
    sBytes = Long.parseLong(args[3]);
    if (args.length == 5) {
      sCheckData = Boolean.parseBoolean(args[4]);
    }

    LOG.info("Going to " + sOperation + " " + sFiles.length + " files with " + sBytes + " bytes, "
        + args[2]);
    if (sOperation.equals("read")) {
      readFiles();
    } else {
      createFiles();
      writeFiles();
    }
    sTachyonClient.close();
    System.exit(0);
  }

  public static void createFiles() throws IOException {
    for (int i = 0; i < sFiles.length; i ++) {
      int fileId = sTachyonClient.createFile(sFiles[i]);
      LOG.info("File " + sFiles[i] + " created with id " + fileId);
    }
  }

  public static void writeFiles() throws IOException {
    for (int i = 0; i < sFiles.length; i ++) {
      TachyonFile file = sTachyonClient.getFile(sFiles[i]);
      OutStream os = file.getOutStream(WriteType.MUST_CACHE);
      //
      for (long k = 0; k < sBytes; k ++) {
        os.write((byte) k);
      }
      os.close();

      LOG.info("Write to file " + sFiles[i] + " " + sBytes + " bytes");
    }
  }

  public static void readFiles() throws IOException {
    try {
      for (int i = 0; i < sFiles.length; i ++) {
        long time = 0;
        int read = 0;
        TachyonFile file = sTachyonClient.getFile(sFiles[i]);
        LOG.info("Going to read file " + sFiles[i] + " size " + file.length());
        time = System.nanoTime();
        InStream is = file.getInStream(ReadType.NO_CACHE);
        if (sCheckData) {
          LOG.info("validating data");
          long k = 0;
          while (read != -1) {
            read = is.read();
            if (read != -1 && (byte) read != (byte) k) {
              LOG.error("ERROR IN TEST, got " + (byte) read + " expected " + (byte) k);
              return;
            }
            k ++;
          }
        } else {
          byte[] buf = new byte[4 * 1024];
          while (read != -1) {
            read = is.read(buf);
          }
        }
        time = System.nanoTime() - time;
        System.out.println("Finished reading file " + sFiles[i] + " time " + time + " \n");
      }
    } catch (Exception e) {
      LOG.error("got Exception in test " + e.toString());
      StackTraceElement[] stackTraceElements = e.getStackTrace();
      for (StackTraceElement stackTrace : stackTraceElements) {
        LOG.error("\t" + stackTrace.toString());
      }
      System.exit(1);
    }
  }
}
