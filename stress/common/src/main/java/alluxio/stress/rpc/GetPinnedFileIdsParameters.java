package alluxio.stress.rpc;

import com.beust.jcommander.Parameter;

public class GetPinnedFileIdsParameters extends RpcBenchParameters {
  @Parameter(names = {"--num-files"},
          description = "The number of pinned files")
  public int mNumFiles = 100;

  @Parameter(names = {"--base-dir"},
      description = "The base directory path URI to perform operations in")
  public String mBasePath = "/get-pin-list-bench-base";
}
