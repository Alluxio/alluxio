/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.stress.worker;

import alluxio.collections.Pair;
import alluxio.stress.BaseParameters;
import alluxio.stress.Parameters;
import alluxio.stress.Summary;
import alluxio.stress.graph.BarGraph;
import alluxio.stress.graph.Graph;
import alluxio.util.FormatUtils;
import alluxio.util.JsonSerializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The summary for the UFS I/O throughput test.
 */
public class IOTaskSummary implements Summary {
  private static final Logger LOG = LoggerFactory.getLogger(IOTaskSummary.class);
  private List<IOTaskResult.Point> mPoints;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private UfsIOParameters mParameters;
  private SpeedStat mReadSpeedStat;
  private SpeedStat mWriteSpeedStat;

  /**
   * Used for deserialization.
   * */
  @JsonCreator
  public IOTaskSummary() {}

  /**
   * @param result the {@link IOTaskResult} to summarize
   * */
  public IOTaskSummary(IOTaskResult result) {
    mPoints = new ArrayList<>(result.getPoints());
    mErrors = new ArrayList<>(result.getErrors());
    mBaseParameters = result.getBaseParameters();
    mParameters = result.getParameters();
    calculateStats();
  }

  /**
   * @return the points recorded
   * */
  public List<IOTaskResult.Point> getPoints() {
    return mPoints;
  }

  /**
   * @param points the data points
   * */
  public void setPoints(List<IOTaskResult.Point> points) {
    mPoints = points;
  }

  /**
   * @return the errors recorded
   * */
  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the errors
   * */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  /**
   * @return the {@link BaseParameters}
   * */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the {@link BaseParameters}
   * */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the task specific {@link UfsIOParameters}
   * */
  public UfsIOParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link UfsIOParameters}
   * */
  public void setParameters(UfsIOParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the {@link SpeedStat} for read speed
   * */
  public SpeedStat getReadSpeedStat() {
    return mReadSpeedStat;
  }

  /**
   * @param stat the {@link SpeedStat} for read stats
   * */
  public void setReadSpeedStat(SpeedStat stat) {
    mReadSpeedStat = stat;
  }

  /**
   * @return the {@link SpeedStat} for write speed
   * */
  public SpeedStat getWriteSpeedStat() {
    return mWriteSpeedStat;
  }

  /**
   * @param stat the {@link SpeedStat} for write stats
   * */
  public void setWriteSpeedStat(SpeedStat stat) {
    mWriteSpeedStat = stat;
  }

  /**
   * An object representation of all the statistics we need
   * from this I/O test.
   * */
  public static class SpeedStat implements JsonSerializable {
    public double mTotalDurationSeconds;
    public long mTotalSizeBytes;
    public double mMaxSpeedMbps;
    public double mMinSpeedMbps;
    public double mAvgSpeedMbps;
    public double mStdDev;

    /**
     * An empty constructor.
     * */
    public SpeedStat() {}

    @Override
    public String toString() {
      return String.format("{totalDuration=%ss, totalSize=%s, maxSpeed=%sMB/s, "
                      + "minSpeed=%sMB/s, " + "avgSpeed=%sMB/s, stdDev=%s}",
              mTotalDurationSeconds, FormatUtils.getSizeFromBytes(mTotalSizeBytes),
              mMaxSpeedMbps, mMinSpeedMbps, mAvgSpeedMbps, mStdDev);
    }
  }

  /**
   * The points must be valid (duration not equal to 0).
   */
  private static SpeedStat calculateStat(List<IOTaskResult.Point> points) {
    SpeedStat result = new SpeedStat();
    if (points.size() == 0) {
      return result;
    }

    double totalDuration = 0.0;
    long totalSize = 0L;
    double[] speeds = new double[points.size()];
    double maxSpeed = 0.0;
    double minSpeed = Double.MAX_VALUE;
    int i = 0;
    for (IOTaskResult.Point p : points) {
      totalDuration += p.mDurationSeconds;
      totalSize += p.mDataSizeBytes;
      double speed = p.mDataSizeBytes / (p.mDurationSeconds * 1024 * 1024); // convert B/s to MB/s
      maxSpeed = Math.max(maxSpeed, speed);
      minSpeed = Math.min(minSpeed, speed);
      speeds[i++] = speed;
    }
    double avgSpeed = totalSize / (totalDuration * 1024 * 1024); // convert B/s to MB/s
    double var = 0;
    for (double s : speeds) {
      var += (s - avgSpeed) * (s - avgSpeed);
    }

    result.mTotalDurationSeconds = totalDuration;
    result.mTotalSizeBytes = totalSize;
    result.mMaxSpeedMbps = maxSpeed;
    result.mMinSpeedMbps = Double.compare(minSpeed, Double.MAX_VALUE) == 0 ? 0.0 : minSpeed;
    result.mAvgSpeedMbps = avgSpeed;
    result.mStdDev = Math.sqrt(var);

    return result;
  }

  private void calculateStats() {
    List<IOTaskResult.Point> readPoints = mPoints.stream().filter((p) ->
            p.mMode == IOTaskResult.IOMode.READ && p.mDurationSeconds > 0)
            .collect(Collectors.toList());
    mReadSpeedStat = calculateStat(readPoints);

    List<IOTaskResult.Point> writePoints = mPoints.stream().filter((p) ->
            p.mMode == IOTaskResult.IOMode.WRITE && p.mDurationSeconds > 0)
            .collect(Collectors.toList());
    mWriteSpeedStat = calculateStat(writePoints);
  }

  @Override
  public GraphGenerator graphGenerator() {
    return new GraphGenerator();
  }

  @Override
  public String toString() {
    return String.format("IOTaskSummary: {Points=%s, Errors=%s}%n",
            mPoints, mErrors);
  }

  /**
   * A graph generator for the statistics collected.
   * */
  public static final class GraphGenerator extends alluxio.stress.GraphGenerator {
    @Override
    public List<Graph> generate(List<? extends Summary> results) {
      List<Graph> graphs = new ArrayList<>();
      List<IOTaskSummary> summaries =
              results.stream().map(x -> (IOTaskSummary) x).collect(Collectors.toList());

      if (summaries.isEmpty()) {
        LOG.info("No summaries to generate.");
        return graphs;
      }

      // first() is the list of common field names, second() is the list of unique field names
      Pair<List<String>, List<String>> fieldNames = Parameters.partitionFieldNames(
              summaries.stream().map(x -> x.mParameters).collect(Collectors.toList()));

      // Split up common description into 100 character chunks, for the sub title
      List<String> subTitle = new ArrayList<>(Splitter.fixedLength(100).splitToList(
              summaries.get(0).mParameters.getDescription(fieldNames.getFirst())));
      for (IOTaskSummary summary : summaries) {
        String series = summary.mParameters.getDescription(fieldNames.getSecond());
        subTitle.add(series);
      }

      BarGraph speedGraph = new BarGraph("Read/Write speed",
              subTitle, "Avg speed in MB/s");
      BarGraph stdDevGraph = new BarGraph("Read/Write speed standard deviation",
              subTitle, "Standard deviation in speed");
      for (IOTaskSummary summary : summaries) {
        String series = summary.mParameters.getDescription(fieldNames.getSecond());
        // read stat
        BarGraph.Data readSpeed = new BarGraph.Data();
        BarGraph.Data readStdDev = new BarGraph.Data();
        SpeedStat readStat = summary.getReadSpeedStat();
        readSpeed.addData(readStat.mAvgSpeedMbps);
        readStdDev.addData(readStat.mStdDev);
        speedGraph.addDataSeries("Read " + series, readSpeed);
        stdDevGraph.addDataSeries("Read " + series, readStdDev);

        // write stat
        BarGraph.Data writeSpeed = new BarGraph.Data();
        BarGraph.Data writeStdDev = new BarGraph.Data();
        SpeedStat writeStat = summary.getWriteSpeedStat();
        writeSpeed.addData(writeStat.mAvgSpeedMbps);
        writeStdDev.addData(writeStat.mStdDev);
        speedGraph.addDataSeries("Write " + series, writeSpeed);
        stdDevGraph.addDataSeries("Write " + series, writeStdDev);
      }
      graphs.add(speedGraph);
      graphs.add(stdDevGraph);

      return graphs;
    }
  }
}
