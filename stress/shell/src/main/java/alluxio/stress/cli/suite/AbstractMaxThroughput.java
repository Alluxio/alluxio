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

package alluxio.stress.cli.suite;

import alluxio.stress.common.AbstractMaxThroughputSummary;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.common.GeneralParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * abstract class for MaxThroughput stressBench.
 * @param <T> the MaxThroughput bench result summary
 * @param <S> the general Bench Summary with common method
 * @param <P> the stress bench parameter
 */
public abstract class AbstractMaxThroughput<T extends AbstractMaxThroughputSummary<P, S>,
    S extends GeneralBenchSummary, P extends GeneralParameters> extends Suite<T> {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractMaxThroughput.class);

  protected int getBestThroughput(int initialThroughput, T summary, List<String> baseArgs,
      int numWorkers) throws Exception {
    int lower = 0;
    int upper = Integer.MAX_VALUE;
    // use the input target throughput as the starting point
    int next = initialThroughput;
    int best = 0;
    while (true) {
      int perWorkerThroughput = next / numWorkers;
      int requestedThroughput = perWorkerThroughput * numWorkers;

      if (perWorkerThroughput == 0) {
        // Cannot run with a target of 0
        break;
      }

      List<String> newArgs = new ArrayList<>(baseArgs);
      updateArgValue(newArgs, "--target-throughput", Integer.toString(perWorkerThroughput));

      S mbr = runSingleTest(newArgs);

      int current = next;
      final float actualThroughput = mbr.getThroughput();
      if ((actualThroughput > requestedThroughput)
          || ((requestedThroughput - actualThroughput) / (float) requestedThroughput) < 0.02) {
        // the throughput was achieved. increase.
        summary.addPassedRun(current, mbr);

        best = current;
        // update the lower bound.
        lower = current;

        if (upper == Integer.MAX_VALUE) {
          next *= 2;
        } else {
          next = (next + upper) / 2;
        }
      } else {
        // Failed to achieve the target throughput. update the upper bound.
        summary.addFailedRun(current, mbr);

        upper = current;
        // throughput was not achieved. decrease.
        next = (lower + next) / 2;
      }
      LOG.info("target: " + requestedThroughput + " actual: " + actualThroughput + " [" + lower
          + " " + next + " " + upper + "]");
      for (String error : mbr.collectErrors()) {
        LOG.error(String.format("%s", error));
      }
      if (Math.abs(current - next) / (float) current <= 0.02) {
        break;
      }
    }
    return best;
  }

  private void updateArgValue(List<String> args, String argName, String argValue) {
    int index = args.indexOf(argName);
    if (index == -1) {
      // arg not found
      args.add(argName);
      args.add(argValue);
      return;
    }
    if (index + 1 < args.size()) {
      // arg found and next index is valid
      args.set(index + 1, argValue);
    } else {
      // the next index is out of bounds
    }
  }

  /**
   * @param args the args
   * @return the results
   */
  protected abstract S runSingleTest(List<String> args) throws Exception;
}
