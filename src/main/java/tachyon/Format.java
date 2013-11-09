package tachyon;

import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;

/**
 * Format Tachyon File System.
 */
public class Format {
  public static void main(String[] args) throws IOException {
    if (args.length != 0) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION +
          "-jar-with-dependencies.jar tachyon.Format");
      System.exit(-1);
    }

    MasterConf masterConf = MasterConf.get();
    UnderFileSystem ufs = UnderFileSystem.get(masterConf.JOURNAL_FOLDER);
    System.out.println("Deleting " + masterConf.JOURNAL_FOLDER);
    if (ufs.exists(masterConf.JOURNAL_FOLDER) && !ufs.delete(masterConf.JOURNAL_FOLDER, true)) {
      System.out.println("Failed to remove " + masterConf.JOURNAL_FOLDER);
    }
    ufs.mkdirs(masterConf.JOURNAL_FOLDER, true);

    CommonConf commonConf = CommonConf.get();
    String folder = commonConf.UNDERFS_DATA_FOLDER;
    ufs = UnderFileSystem.get(folder);
    System.out.println("Formatting " + folder);
    ufs.delete(folder, true);
    if (!ufs.mkdirs(folder, true)) {
      System.out.println("Failed to create " + folder);
    }

    folder = commonConf.UNDERFS_WORKERS_FOLDER;
    System.out.println("Formatting " + folder);
    ufs.delete(folder, true);
    if (!ufs.mkdirs(folder, true)) {
      System.out.println("Failed to create " + folder);
    }
  }
}