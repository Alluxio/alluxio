package alluxio.stress.rpc;

import alluxio.stress.Parameters;
import com.beust.jcommander.Parameter;

public class GetPinnedFileIdsParameters extends Parameters {
  // TODO: these two parameters are duplicate, try to refactor

  @Parameter(names = {"--concurrency"},
          description = "simulate this many workers on one machine")
  public int mConcurrency = 2;

  @Parameter(names = {"--duration"},
          description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  // TODO(bowen): define your parameters here, similar to RegisterWorkerParameters
}
