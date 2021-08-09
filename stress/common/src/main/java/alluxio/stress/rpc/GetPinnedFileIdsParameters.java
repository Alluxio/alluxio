package alluxio.stress.rpc;

import com.beust.jcommander.Parameter;

/**
 * Parameters for the stress test on GetPinnedFileIds RPC.
 */
public class GetPinnedFileIdsParameters extends RpcBenchParameters {
  @Parameter(names = {"--num-files"},
      description = "The number of pinned files to generate in the test.")
  public int mNumFiles = 100;

  @Parameter(names = {"--base-dir"},
      description = "The test directory will be created and hold all the test files. "
          + "At the end of the test, this directory will be removed.")
  public String mBasePath = "/get-pin-list-bench-base";
}
