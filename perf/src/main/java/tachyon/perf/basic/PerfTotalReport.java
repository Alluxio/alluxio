package tachyon.perf.basic;

import java.io.File;
import java.io.IOException;

/**
 * The abstract class for the total report, which is used to generate a test report by the
 * TachyonPerfCollector tool. For new test, you should create a new class which extends this.
 */
public abstract class PerfTotalReport {

  protected String mTestCase;

  public void initialSet(String testCase) {
    mTestCase = testCase;
  }

  /**
   * Load the contexts of all the task slaves and initial this total report.
   * 
   * @param taskContexts the contexts for all the task slaves
   * @throws IOException
   */
  public abstract void initialFromTaskContexts(PerfTaskContext[] taskContexts) throws IOException;

  /**
   * Output this total report to file.
   * 
   * @param file the output file
   * @throws IOException
   */
  public abstract void writeToFile(File file) throws IOException;
}
