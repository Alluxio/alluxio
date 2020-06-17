package alluxio.cli;

import com.beust.jcommander.JCommander;

public class UfsTests {
  private static String getHelpMessage() {
    return "Test description:\n"
            + "Test the integration between Alluxio and the under filesystem. "
            + "If the given under filesystem is S3, this test can also be used as "
            + "a S3 compatibility test to test if the target under filesystem can "
            + "fulfill the minimum S3 compatibility requirements in order to "
            + "work well with Alluxio through Alluxio's integration with S3. \n"
            + "Command line example: 'bin/alluxio runUfsTests --path s3://testPath "
            + "-Daws.accessKeyId=<accessKeyId> -Daws.secretKeyId=<secretKeyId>"
            + "-Dalluxio.underfs.s3.endpoint=<endpoint_url> "
            + "-Dalluxio.underfs.s3.disable.dns.buckets=true'";
  }

  /**
   * @param args the input arguments
   */
  public static void main(String[] args) throws Exception {
    UnderFileSystemContractTest test = new UnderFileSystemContractTest();
    JCommander jc = new JCommander(test);
    jc.setProgramName(UnderFileSystemContractTest.class.getName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      jc.usage();
      System.out.println(getHelpMessage());
      System.exit(1);
    }
    if (test.needHelp()) {
      jc.usage();
      System.out.println(getHelpMessage());
    } else {
      test.run();
    }
  }
}
