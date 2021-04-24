package alluxio.stress.worker;

import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;

import java.util.List;

public class RpcTaskSummary implements Summary {
  private List<RpcTaskResult.Point> mPoints;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private RpcParameters mParameters;

  public RpcTaskSummary(RpcTaskResult r) {
    mParameters = r.getParameters();
    mBaseParameters = r.getBaseParameters();
    mErrors = r.getErrors();
    mPoints = r.getPoints();
  }

  @Override
  public GraphGenerator graphGenerator() {
    return null;
  }

  @Override
  public String toString() {
    return String.format("RpcTaskSummary: {Points=%s, Errors=%s}%n",
            mPoints, mErrors);
  }
}
