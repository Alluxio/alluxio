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

package alluxio.table.under.glue;

import alluxio.table.common.udb.UdbProperty;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This contains all the properties for Glue UDB.
 */
public class Property extends UdbProperty {
  private static final Logger LOG = LoggerFactory.getLogger(Property.class);
  /** A map from default property key's string name to the key. */
  private static final Map<String, Property> DEFAULT_KEYS_MAP = new ConcurrentHashMap<>();

  /**
   * Create a alluxio.table.under.glue.Property instance.
   *
   * @param name the property name
   * @param description the property description
   * @param defaultValue the default value
   */
  public Property(String name, String description, String defaultValue) {
    super(name, description, defaultValue);
  }

  /**
   * UDB property builder.
   */
  public static final class Builder {
    private String mName;
    private String mDescription;
    private String mDefaultValue;

    /**
     * @param name name of property
     */
    public Builder(String name) {
      mName = name;
    }

    /**
     * @param name name for the property
     * @return the updated builder instance
     */
    public Builder setName(String name) {
      mName = name;
      return this;
    }

    /**
     * @param defaultValue the property's default value
     * @return the updated builder instance
     */
    public Builder setDefaultValue(String defaultValue) {
      mDefaultValue = defaultValue;
      return this;
    }

    /**
     * @param description of the property
     * @return the updated builder instance
     */
    public Builder setDescription(String description) {
      mDescription = description;
      return this;
    }

    /**
     * Register the unregistered udb property.
     *
     * @return registered udb property
     */
    public Property build() {
      Property property = buildUnregistered();
      Preconditions.checkState(
          Property.register(property),
          "Cannot register existing alluxio.table.under.glue.Property \"%s\"", mName);
      return property;
    }

    /**
     * Creates the Udb alluxio.table.under.glue.Property
     * without registering it with default property list.
     *
     * @return udb property
     */
    public Property buildUnregistered() {
      Property property = new Property(mName, mDescription, mDefaultValue);
      return property;
    }
  }

  /**
   * Registers the given UDB alluxio.table.under.glue.Property to the global map.
   *
   * @param Property the udb property
   * @return whether the udb property is successfully registered
   */
  @VisibleForTesting
  public static boolean register(Property Property) {
    String name = Property.getName();
    if (DEFAULT_KEYS_MAP.containsKey(name)) {
      return false;
    }

    DEFAULT_KEYS_MAP.put(name, Property);
    return true;
  }

  /**
   * Unregisters the given key from the global map.
   *
   * @param Property the property to unregister
   */
  @VisibleForTesting
  public static void unregister(Property Property) {
    String name = Property.getName();
    DEFAULT_KEYS_MAP.remove(name);
  }

  public static final Property MAX_GLUE_CONNECTION =
      new Builder(Name.MAX_GLUE_CONNECTION)
          .setDefaultValue("5")
          .setDescription("The maximum number of connection to glue metastore.")
          .build();

  public static final Property MAX_GLUE_FETCH_PARTITIONS =
      new Builder(Name.MAX_GLUE_FETCH_PARTITIONS)
          .setDefaultValue("512")
          .setDescription("The maximum number of partitions to return in a single response.")
          .build();

  public static final Property GLUE_REGION =
      new Builder(Name.GLUE_REGION)
          .setDefaultValue("")
          .setDescription("The regional endpoint for client service calls.")
          .build();

  // TODO(shouwei): check the necessity of catalogid
  public static final Property CATALOG_ID =
      new Builder(Name.CATALOG_ID)
          .setDefaultValue("")
          .setDescription("The catalog id of aws glue.")
          .build();

  public static final Property AWS_GLUE_ACCESS_KEY =
      new Builder(Name.AWS_GLUE_ACCESS_KEY)
          .setDefaultValue("")
          .setDescription("The access key to access the aws glue.")
          .build();

  public static final Property AWS_GLUE_SECRET_KEY =
      new Builder(Name.AWS_GLUE_SECRET_KEY)
          .setDefaultValue("")
          .setDescription("The secret key to access the aws glue.")
          .build();

  public static final Property AWS_PROXY_PROTOCOL =
      new Builder(Name.AWS_PROXY_PROTOCOL)
          .setDefaultValue("HTTP")
          .setDescription("The Protocol to use for connecting to the proxy.")
          .build();

  public static final Property AWS_PROXY_HOST =
      new Builder(Name.AWS_PROXY_HOST)
          .setDefaultValue("")
          .setDescription("The proxy host the client will connect through.")
          .build();

  public static final Property AWS_PROXY_PORT =
      new Builder(Name.AWS_PROXY_PORT)
          .setDefaultValue("")
          .setDescription("The proxy port the client will connect through.")
          .build();

  public static final Property AWS_PROXY_USER_NAME =
      new Builder(Name.AWS_PROXY_USER_NAME)
          .setDefaultValue("")
          .setDescription("The proxy user name.")
          .build();

  public static final Property AWS_PROXY_PASSWORD =
      new Builder(Name.AWS_PROXY_PASSWORD)
          .setDefaultValue("")
          .setDescription("The proxy password.")
          .build();

  public static final Property TABLE_COLUMN_STATISTICS_ENABLE =
      new Builder(Name.TABLE_COLUMN_STATISTICS_ENABLE)
          .setDefaultValue("false")
          .setDescription("Enable Glue table column statistics.")
          .build();

  public static final Property PARTITION_COLUMN_STATISTICS_ENABLE =
      new Builder(Name.PARTITION_COLUMN_STATISTICS_ENABLE)
          .setDefaultValue("false")
          .setDescription("Enable Glue partition column statistics.")
          .build();

  /**
   * @return the name of alluxio.table.under.glue.Property
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the description of a property
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * @return the default value of udb property or null if value not set
   */
  @Nullable
  @Override
  public String getDefaultValue() {
    Object defaultValue = mDefaultValue;
    return defaultValue == null ? null : defaultValue.toString();
  }

  /**
   * Corresponding configurations of GLUE configurations.
   */
  public static final class Name {
    // AWS Glue related properties
    public static final String MAX_GLUE_CONNECTION = "max.connection";
    public static final String MAX_GLUE_FETCH_PARTITIONS = "partitions.fetch.max";
    public static final String GLUE_REGION = "aws.region";
    public static final String CATALOG_ID = "aws.catalog.id";
    public static final String AWS_GLUE_ACCESS_KEY = "aws.accesskey";
    public static final String AWS_GLUE_SECRET_KEY = "aws.secretkey";
    public static final String AWS_PROXY_PROTOCOL = "aws.proxy.protocol";
    public static final String AWS_PROXY_HOST = "aws.proxy.host";
    public static final String AWS_PROXY_PORT = "aws.proxy.port";
    public static final String AWS_PROXY_USER_NAME = "aws.proxy.username";
    public static final String AWS_PROXY_PASSWORD = "aws.proxy.password";
    public static final String TABLE_COLUMN_STATISTICS_ENABLE = "table.column.statistics";
    public static final String PARTITION_COLUMN_STATISTICS_ENABLE = "partition.column.statistics";
  }
}
