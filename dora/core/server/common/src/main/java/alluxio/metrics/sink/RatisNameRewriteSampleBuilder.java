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

package alluxio.metrics.sink;

import static org.apache.ratis.metrics.RatisMetrics.RATIS_APPLICATION_NAME_METRICS;

import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Collect Dropwizard metrics and rename ratis specific metrics.
 */
public class RatisNameRewriteSampleBuilder extends DefaultSampleBuilder {

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisNameRewriteSampleBuilder.class);

  private List<Pattern> mFollowerPatterns = new ArrayList<>();

  protected RatisNameRewriteSampleBuilder() {
    mFollowerPatterns
        .add(Pattern.compile(
            "grpc_log_appender_follower_(.*)_(latency|success|inconsistency)"
                + ".*"));
    mFollowerPatterns
        .add(Pattern.compile("follower_([^_]*)_.*"));
    mFollowerPatterns.add(Pattern.compile("([^_]*)_peerCommitIndex"));
  }

  @Override
  public Sample createSample(String dropwizardName, String nameSuffix,
      List<String> additionalLabelNames, List<String> additionalLabelValues,
      double value) {
    //this is a ratis metrics, where the second part is an instance id.
    if (dropwizardName.startsWith(RATIS_APPLICATION_NAME_METRICS)) {
      List<String> names = new ArrayList<>(additionalLabelNames);
      List<String> values = new ArrayList<>(additionalLabelValues);
      String name = normalizeRatisMetric(dropwizardName, names, values);

      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Ratis dropwizard {} metrics are converted to {} with tag "
                + "keys/values {},{}", dropwizardName, name, names, values);
      }
      return super.createSample(name, nameSuffix, names, values, value);
    } else {
      return super
          .createSample(dropwizardName, nameSuffix, additionalLabelNames,
              additionalLabelValues, value);
    }
  }

  protected String normalizeRatisMetric(String dropwizardName,
      List<String> names,
      List<String> values) {

    List<String> nameParts =
        new ArrayList<>(Arrays.asList(dropwizardName.split("\\.")));
    //second part is id or id@group_id
    if (nameParts.size() > 2) {
      String[] identifiers = nameParts.get(2).split("@");
      names.add("instance");
      values.add(identifiers[0]);
      if (identifiers.length > 1) {
        names.add("group");
        values.add(identifiers[1]);
      }
      nameParts.remove(2);
    }

    if (nameParts.size() > 2) {
      for (Pattern pattern : mFollowerPatterns) {
        Matcher matcher = pattern.matcher(nameParts.get(2));
        if (matcher.matches()) {
          names.add("follower");
          String followerId = matcher.group(1);
          values.add(followerId);
          nameParts.set(2, nameParts.get(2).replace(followerId + "_", ""));
        }
      }
    }
    return Strings.join(nameParts, '.');
  }
}
