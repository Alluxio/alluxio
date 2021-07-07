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

package alluxio.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * This class contains old {@link PropertyKey}s which have been removed from use
 *
 * This class is used to track keys which were deprecated in previous versions and subsequently
 * removed in a future version. We still keep them here so that it is possible to provide users with
 * useful information if they are found to be using an outdated property key.
 *
 * Being removed and still used by an application denotes an error.
 *
 * @see InstancedConfiguration#validate()
 * @see PropertyKey#fromString(String)
 */
public final class RemovedKey {

  private static final String V2_0_0 = "v2.0.0";
  private static final String V2_1_0 = "v2.1.0";
  private static final String V2_3_0 = "v2.3.0";
  private static final String V2_4_0 = "v2.4.0";
  private static final String V2_6_0 = "v2.6.0";

  /**
   * @param version the version since which a property has been removed
   *
   * @return a warning message indicating a property has been removed since a given version
   */
  private static String removedSince(String version) {
    return String.format("this property has been removed since %s.", version);
  }

  /**
   * @param version the version since which a property has been removed
   * @param newProperty the new property to use instead
   * @return a warning message indicating a property has been removed since a given version
   */
  private static String replacedSince(String version, String newProperty) {
    return String.format("this property has been removed since %s, use %s instead.", version,
        newProperty);
  }

  private static final Map<String, String> REMOVED_KEYS = new HashMap<String, String>(20) {
    {
      put("alluxio.keyvalue.enabled", removedSince(V2_0_0));
      put("alluxio.keyvalue.partition.size.bytes.max", removedSince(V2_0_0));
      put("alluxio.master.client.socket.cleanup.interval", removedSince(V2_0_0));
      put("alluxio.master.connection.timeout", removedSince(V2_0_0));
      put("alluxio.master.file.async.persist.handler", removedSince(V2_0_0));
      put("alluxio.master.heartbeat.interval",
          replacedSince(V2_0_0, PropertyKey.MASTER_LOST_WORKER_FILE_DETECTION_INTERVAL.getName()));
      put("alluxio.master.journal.formatter.class",
          "v2.0 removed the ability to specify the master journal formatter");
      put("alluxio.master.lineage.checkpoint.class", removedSince(V2_0_0));
      put("alluxio.master.lineage.checkpoint.interval", removedSince(V2_0_0));
      put("alluxio.master.lineage.recompute.interval", removedSince(V2_0_0));
      put("alluxio.master.lineage.recompute.log.path", removedSince(V2_0_0));
      put("alluxio.master.master.heartbeat.interval", replacedSince(V2_0_0,
          PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL.getName()));
      put("alluxio.master.startup.consistency.check.enabled", removedSince(V2_0_0));
      put("alluxio.master.thrift.shutdown.timeout", removedSince(V2_0_0));
      put("alluxio.master.retry", removedSince(V2_0_0));
      put("alluxio.master.worker.threads.max", removedSince(V2_0_0));
      put("alluxio.master.worker.threads.min", removedSince(V2_0_0));
      put("alluxio.master.embedded.journal.appender.batch.size", removedSince(V2_4_0));
      put("alluxio.master.embedded.journal.storage.level", removedSince(V2_4_0));
      put("alluxio.master.embedded.journal.shutdown.timeout", removedSince(V2_4_0));
      put("alluxio.master.embedded.journal.triggered.snapshot.wait.timeout", removedSince(V2_4_0));
      put("alluxio.network.netty.heartbeat.timeout", removedSince(V2_0_0));
      put("alluxio.network.thrift.frame.size.bytes.max", removedSince(V2_0_0));
      put("alluxio.underfs.object.store.read.retry.base.sleep", removedSince(V2_0_0));
      put("alluxio.underfs.object.store.read.retry.max.num", removedSince(V2_0_0));
      put("alluxio.underfs.object.store.read.retry.max.sleep", removedSince(V2_0_0));
      put("alluxio.underfs.s3a.consistency.timeout", removedSince(V2_0_0));
      put("alluxio.security.authentication.socket.timeout", removedSince(V2_0_0));
      put("alluxio.security.authentication.socket.timeout.ms", removedSince(V2_0_0));
      put("alluxio.user.block.remote.reader.class", removedSince(V2_0_0));
      put("alluxio.user.block.remote.writer.class", removedSince(V2_0_0));
      put("alluxio.user.block.worker.client.pool.size.max", removedSince(V2_0_0));
      put("alluxio.user.block.worker.client.threads", removedSince(V2_0_0));
      put("alluxio.user.failed.space.request.limits", removedSince(V2_0_0));
      put("alluxio.user.file.cache.partially.read.block", removedSince(V2_0_0));
      put("alluxio.user.file.copyfromlocal.write.location.policy.class", replacedSince(V2_0_0,
          PropertyKey.USER_FILE_COPYFROMLOCAL_BLOCK_LOCATION_POLICY.getName()));
      put("alluxio.user.file.seek.buffer.size.bytes", removedSince(V2_0_0));
      put("alluxio.user.file.write.avoid.eviction.policy.reserved.size.bytes",
          replacedSince(V2_0_0,
              PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES.getName()));
      put("alluxio.user.file.write.location.policy.class",
          replacedSince(V2_0_0, PropertyKey.USER_BLOCK_WRITE_LOCATION_POLICY.getName()));
      put("alluxio.user.heartbeat.interval", removedSince(V2_0_0));
      put("alluxio.user.hostname",
          replacedSince(V2_0_0, PropertyKey.LOCALITY_TIER_NODE.getName()));
      put("alluxio.user.lineage.enabled", removedSince(V2_0_0));
      put("alluxio.user.lineage.master.client.threads", removedSince(V2_0_0));
      put("alluxio.user.local.reader.packet.size.bytes", removedSince(V2_0_0));
      put("alluxio.user.local.writer.packet.size.bytes", removedSince(V2_0_0));
      put("alluxio.user.network.netty.channel.pool.disabled", removedSince(V2_0_0));
      put("alluxio.user.network.netty.channel.pool.gc.threshold", removedSince(V2_0_0));
      put("alluxio.user.network.netty.channel.pool.size.max", removedSince(V2_0_0));
      put("alluxio.user.network.netty.channel.pool.size.min", removedSince(V2_0_0));
      put("alluxio.user.network.netty.reader.buffer.size.packets", removedSince(V2_0_0));
      put("alluxio.user.network.netty.reader.packet.size.bytes", removedSince(V2_0_0));
      put("alluxio.user.network.netty.writer.buffer.size.packets", removedSince(V2_0_0));
      put("alluxio.user.network.netty.writer.close.timeout", removedSince(V2_0_0));
      put("alluxio.user.network.netty.writer.packet.size.bytes", removedSince(V2_0_0));
      put("alluxio.user.network.socket.timeout", removedSince(V2_0_0));
      put("alluxio.user.rpc.retry.max.num.retry", removedSince(V2_0_0));
      put("alluxio.user.ufs.delegation.read.buffer.size.bytes", removedSince(V2_0_0));
      put("alluxio.user.ufs.delegation.write.buffer.size.bytes", removedSince(V2_0_0));
      put("alluxio.user.ufs.file.reader.class", removedSince(V2_0_0));
      put("alluxio.user.ufs.file.writer.class", removedSince(V2_0_0));
      put("alluxio.web.temp.path", removedSince(V2_0_0));
      put("alluxio.worker.block.threads.max", removedSince(V2_0_0));
      put("alluxio.worker.block.threads.min", removedSince(V2_0_0));
      put("alluxio.worker.data.bind.host", removedSince(V2_0_0));
      put("alluxio.worker.data.hostname", removedSince(V2_0_0));
      put("alluxio.worker.data.port", replacedSince(V2_0_0, PropertyKey.WORKER_RPC_PORT.getName()));
      put("alluxio.worker.data.server.class", removedSince(V2_6_0));
      put("alluxio.worker.filesystem.heartbeat.interval", removedSince(V2_1_0));
      put("alluxio.worker.file.buffer.size", removedSince(V2_6_0));
      put("alluxio.worker.file.persist.pool.size", removedSince(V2_1_0));
      put("alluxio.worker.file.persist.rate.limit", removedSince(V2_1_0));
      put("alluxio.worker.file.persist.rate.limit.enabled", removedSince(V2_1_0));
      put("alluxio.worker.memory.size", replacedSince(V2_4_0,
          PropertyKey.Name.WORKER_RAMDISK_SIZE));
      put("alluxio.worker.network.netty.async.cache.manager.threads.max", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.backlog", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.block.reader.threads.max", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.block.writer.threads.max", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.buffer.receive", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.buffer.send", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.file.transfer", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.file.writer.threads.max", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.reader.buffer.size.packets", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.rpc.threads.max", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.shutdown.timeout", removedSince(V2_0_0));
      put("alluxio.worker.network.netty.writer.buffer.size.packets", removedSince(V2_0_0));
      put("alluxio.worker.tieredstore.reserver.enabled", removedSince(V2_0_0));
      put("alluxio.worker.tieredstore.retry", removedSince(V2_0_0));
      put("alluxio.worker.evictor.lrfu.attenuation.factor", removedSince(V2_3_0));
      put("alluxio.worker.evictor.lrfu.step.factor", removedSince(V2_3_0));
      put("fs.swift.apikey", replacedSince(V2_0_0, PropertyKey.Name.SWIFT_PASSWORD_KEY));
      put("fs.swift.use.public.url", removedSince(V2_0_0));
      put(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(0),
          replacedSince(V2_0_0, "alluxio.worker.tieredstore.level0.watermark.{high/low}.ratio"));
      put(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(1),
          replacedSince(V2_0_0, "alluxio.worker.tieredstore.level1.watermark.{high/low}.ratio"));
      put(Template.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO.format(2),
          replacedSince(V2_0_0, "alluxio.worker.tieredstore.level2.watermark.{high/low}.ratio"));

      put(Name.TEST_REMOVED_KEY, "This key is used only for testing. It is always removed");
    }
  };

  static final class Name {
    public static final String TEST_REMOVED_KEY = "alluxio.test.removed.key";
  }

  static final class Template {

    private static final List<Template> TEMPLATES = new ArrayList<>();

    public static final Template WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO = new Template(
        "alluxio.worker.tieredstore.level%d.reserved.ratio",
        "alluxio\\.worker\\.tieredstore\\.level(\\d+)\\.reserved\\.ratio",
        "The keys associated with this template have been removed");

    private final String mFormat;
    private final Pattern mPattern;
    private final String mMessage;

    private Template(String format, String re, String removalMessage) {
      mFormat = format;
      mPattern = Pattern.compile(re);
      mMessage = removalMessage;
      TEMPLATES.add(this);
    }

    private String format(Object... o) {
      return String.format(mFormat, o);
    }

    private boolean matches(String input) {
      Matcher matcher = mPattern.matcher(input);
      return matcher.matches();
    }
  }

  /**
   * returns whether or not the given property key exists in the removed key list.
   *
   * @param key the property key to check
   * @return whether or not the key has been removed
   */
  static boolean isRemoved(String key) {
    return getMessage(key) != null;
  }

  /**
   * gets the message pertaining to a removed key or template.
   *
   * @param key the property key to check
   * @return whether or not the key has been removed
   */
  @Nullable
  public static String getMessage(String key) {
    String msg;
    if ((msg = REMOVED_KEYS.getOrDefault(key, null)) != null) {
      return msg;
    }

    for (Template t : Template.TEMPLATES) {
      if (t.matches(key)) {
        return t.mMessage;
      }
    }
    return null;
  }

  private RemovedKey() {} // prevent instantiation of this class
}
