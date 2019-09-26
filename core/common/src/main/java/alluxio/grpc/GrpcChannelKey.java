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

package alluxio.grpc;

import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;

import com.google.common.base.MoreObjects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Used to identify a unique {@link GrpcChannel}.
 */
public class GrpcChannelKey {
  private static final Random RANDOM = new Random();

  @IdentityField
  @SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  Long mPoolKey = 0L;
  @IdentityField
  private GrpcServerAddress mServerAddress;
  @IdentityField
  private Optional<Pair<Long, TimeUnit>> mKeepAliveTime = Optional.empty();
  @IdentityField
  private Optional<Pair<Long, TimeUnit>> mKeepAliveTimeout = Optional.empty();
  @IdentityField
  private Optional<Integer> mMaxInboundMessageSize = Optional.empty();
  @IdentityField
  private Optional<Integer> mFlowControlWindow = Optional.empty();
  @IdentityField
  private Optional<Class<? extends io.netty.channel.Channel>> mChannelType = Optional.empty();
  @IdentityField
  private Optional<EventLoopGroup> mEventLoopGroup = Optional.empty();

  private Optional<String> mClientType = Optional.empty();
  /** Unique channel identifier. */
  private UUID mChannelId = UUID.randomUUID();

  private GrpcChannelKey() {}

  /**
   * Creates a {@link GrpcChannelKey}.
   *
   * @param conf the Alluxio configuration
   * @return the created instance
   */
  public static GrpcChannelKey create(AlluxioConfiguration conf) {
    return new GrpcChannelKey();
  }

  /**
   * @return unique identifier for the channel
   */
  public UUID getChannelId() {
    return mChannelId;
  }

  /**
   * @return destination address of the channel
   */
  public GrpcServerAddress getServerAddress() {
    return mServerAddress;
  }

  /**
   * @param address destination address of the channel
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setServerAddress(GrpcServerAddress address) {
    mServerAddress = address;
    return this;
  }

  /**
   * @return max inbound message size for the underlying channel
   */
  public Optional<Integer> getMaxInboundMessageSize() {
    return mMaxInboundMessageSize;
  }

  /**
   * @param maxInboundMessageSize max inbound message size for the underlying channel
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setMaxInboundMessageSize(int maxInboundMessageSize) {
    mMaxInboundMessageSize = Optional.of(maxInboundMessageSize);
    return this;
  }

  /**
   * @return flow control window value for the underlying channel
   */
  public Optional<Integer> getFlowControlWindow() {
    return mFlowControlWindow;
  }

  /**
   * @param flowControlWindow flow control window value for the underlying channel
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setFlowControlWindow(int flowControlWindow) {
    mFlowControlWindow = Optional.of(flowControlWindow);
    return this;
  }

  /**
   * @return channel type for the underlying channel
   */
  public Optional<Class<? extends Channel>> getChannelType() {
    return mChannelType;
  }

  /**
   *
   * @param channelType channel type for the underlying channel
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setChannelType(Class<? extends io.netty.channel.Channel> channelType) {
    mChannelType = Optional.of(channelType);
    return this;
  }

  /**
   * @return event loop group for the underlying channel
   */
  public Optional<EventLoopGroup> getEventLoopGroup() {
    return mEventLoopGroup;
  }

  /**
   *
   * @param eventLoopGroup event loop group for the underlying channel
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setEventLoopGroup(EventLoopGroup eventLoopGroup) {
    mEventLoopGroup = Optional.of(eventLoopGroup);
    return this;
  }

  /**
   * @return human readable name for the channel
   */
  public Optional<String> getChannelName() {
    return mClientType;
  }

  /**
   * Sets human readable name for the channel's client.
   *
   * @param clientType channel client type
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setClientType(String clientType) {
    mClientType = Optional.of(clientType);
    return this;
  }

  /**
   * @return keep alive time for the underlying channel
   */
  public Optional<Pair<Long, TimeUnit>> getKeepAliveTime() {
    return mKeepAliveTime;
  }

  /**
   * @param keepAliveTime keep alive time for the underlying channel
   * @param timeUnit time unit for the keepAliveTime parameter
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setKeepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
    mKeepAliveTime = Optional.of(new Pair<>(keepAliveTime, timeUnit));
    return this;
  }

  /**
   * @return keep alive timeout for the underlying channel
   */
  public Optional<Pair<Long, TimeUnit>> getKeepAliveTimeout() {
    return mKeepAliveTimeout;
  }

  /**
   * @param keepAliveTimeout keep alive timeout for the underlying channel
   * @param timeUnit time unit for the keepAliveTimeout parameter
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setKeepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
    mKeepAliveTimeout = Optional.of(new Pair<>(keepAliveTimeout, timeUnit));
    return this;
  }

  /**
   *
   * @param strategy the pooling strategy
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setPoolingStrategy(PoolingStrategy strategy) {
    // TODO(feng): implement modularized pooling strategies
    switch (strategy) {
      case DEFAULT:
        mPoolKey = 0L;
        break;
      case DISABLED:
        mPoolKey = RANDOM.nextLong();
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid pooling strategy %s", strategy.name()));
    }
    return this;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hashCodebuilder = new HashCodeBuilder();
    for (Field field : this.getClass().getDeclaredFields()) {
      if (field.isAnnotationPresent(IdentityField.class)) {
        try {
          hashCodebuilder.append(field.get(this));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(
              String.format("Failed to calculate hashcode for channel-key: %s", this), e);
        }
      }
    }
    return hashCodebuilder.toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof GrpcChannelKey) {
      GrpcChannelKey otherKey = (GrpcChannelKey) other;
      boolean areEqual = true;
      for (Field field : this.getClass().getDeclaredFields()) {
        if (field.isAnnotationPresent(IdentityField.class)) {
          try {
            areEqual &= field.get(this).equals(field.get(otherKey));
          } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format(
                "Failed to calculate equality between channel-keys source: %s | destination: %s",
                this, otherKey), e);
          }
        }
      }
      return areEqual;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ClientType", getStringFromOptional(mClientType))
        .add("ServerAddress", mServerAddress)
        .add("ChannelId", mChannelId)
        .add("KeepAliveTime", getStringFromOptional(mKeepAliveTime))
        .add("KeepAliveTimeout", getStringFromOptional(mKeepAliveTimeout))
        .add("FlowControlWindow", getStringFromOptional(mFlowControlWindow))
        .add("MaxInboundMessageSize", getStringFromOptional(mMaxInboundMessageSize))
        .add("ChannelType", getStringFromOptional(mChannelType))
        .omitNullValues()
        .toString();
  }

  /**
   * @return short representation of this channel key
   */
  public String toStringShort() {
    return MoreObjects.toStringHelper(this)
        .add("ClientType", getStringFromOptional(mClientType))
        .add("ServerAddress", mServerAddress)
        .add("ChannelId", mChannelId)
        .omitNullValues()
        .toString();
  }

  /**
   * Used to get underlying string representation from {@link Optional} fields.
   *
   * @param field an optional field
   * @return underlying string representation or {@code null} if field is not present
   */
  private String getStringFromOptional(Optional<?> field) {
    if (field.isPresent()) {
      return field.get().toString();
    } else {
      return null;
    }
  }

  /**
   * Enumeration to determine the pooling strategy.
   */
  public enum PoolingStrategy {
    DEFAULT,
    DISABLED
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  /**
   * Used to mark fields in this class that are part of
   * the identity of a channel while pooling channels.
   *
   * Values of fields that are marked with this annotation will be used
   * during {@link #hashCode()} and {@link #equals(Object)}.
   */
  protected @interface IdentityField {
  }
}
