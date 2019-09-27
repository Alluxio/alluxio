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

package alluxio.experimental;

import alluxio.grpc.catalog.FieldSchema;
import alluxio.grpc.catalog.FieldTypeId;
import alluxio.grpc.catalog.Layout;
import alluxio.grpc.catalog.Partition;
import alluxio.grpc.catalog.PartitionInfo;
import alluxio.grpc.catalog.Schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Protobuf related utils.
 */
public final class ProtoUtils {
  static final BiMap<Type.TypeID, FieldTypeId> TYPE_ID_TYPE_MAP =
      ImmutableBiMap.<Type.TypeID, FieldTypeId>builder()
          .put(Type.TypeID.BINARY, FieldTypeId.BINARY)
          .put(Type.TypeID.BOOLEAN, FieldTypeId.BOOLEAN)
          .put(Type.TypeID.INTEGER, FieldTypeId.INTEGER)
          .put(Type.TypeID.LONG, FieldTypeId.LONG)
          .put(Type.TypeID.FLOAT, FieldTypeId.FLOAT)
          .put(Type.TypeID.DOUBLE, FieldTypeId.DOUBLE)
          .put(Type.TypeID.DATE, FieldTypeId.DATE)
          .put(Type.TypeID.TIME, FieldTypeId.TIME)
          .put(Type.TypeID.TIMESTAMP, FieldTypeId.TIMESTAMP)
          .put(Type.TypeID.STRING, FieldTypeId.STRING)
          .put(Type.TypeID.UUID, FieldTypeId.UUID)
          .put(Type.TypeID.FIXED, FieldTypeId.FIXED)
          .put(Type.TypeID.DECIMAL, FieldTypeId.DECIMAL)
          .put(Type.TypeID.STRUCT, FieldTypeId.STRUCT)
          .put(Type.TypeID.LIST, FieldTypeId.LIST)
          .put(Type.TypeID.MAP, FieldTypeId.MAP)
      .build();

  private static FieldSchema toProto(Types.NestedField col) {
    if (col.type().isPrimitiveType()) {
      return FieldSchema.newBuilder().setName(col.name())
              .setType(alluxio.grpc.catalog.Type.newBuilder()
                  .setType(TYPE_ID_TYPE_MAP.get(col.type().typeId()))
                  .setPrimitiveField(col.type().toString()).build())
              .setOptional(col.isOptional())
              .build();
    } else { // col is a nested type
      return FieldSchema.newBuilder().setName(col.name())
          .setType(alluxio.grpc.catalog.Type.newBuilder()
              .setType(TYPE_ID_TYPE_MAP.get(col.type().typeId()))
              .addAllNestedFields(
                  col.type().asNestedType().fields()
                      .stream().map(ProtoUtils::toProto)
                      .collect(Collectors.toList())).build())
          .setOptional(col.isOptional())
          .build();
    }
  }

  /**
   * @param  schema internal representation of the schema
   * @return the protocol buffer version of schema
   */
  public static Schema toProto(org.apache.iceberg.Schema schema) {
    Schema.Builder builder = Schema.newBuilder();
    return builder.addAllCols(schema.columns().stream()
        .map(ProtoUtils::toProto).collect(Collectors.toList())).build();
  }

  private static Type fromProto(alluxio.grpc.catalog.Type type) {
    // TODO(david): replace with a method to detect if a type is a complex type or a primitive type
    if (type.getType().getNumber() < FieldTypeId.STRUCT_VALUE) {
      // primitive type
      return Types.fromPrimitiveString(type.getPrimitiveField());
    } else {
      // complex types
      switch (type.getType()) {
        case STRUCT:
          return Types.StructType.of(type.getNestedFieldsList().stream()
              .map(ProtoUtils::fromProto).collect(Collectors.toList()));
        case LIST:
          Preconditions.checkState(type.getNestedFieldsCount() == 1,
              "List type should have an element field");
          Type listType;
          FieldSchema elem = type.getNestedFields(0);
          if (elem.getOptional()) {
            // if the element is optional
            listType = Types.ListType.ofOptional(elem.getId(), fromProto(elem.getType()));
          } else {
            listType = Types.ListType.ofRequired(elem.getId(), fromProto(elem.getType()));
          }
          return listType;
        case MAP:
          Preconditions.checkState(type.getNestedFieldsCount() == 2,
              "Map type should have a key and a value field");
          Type mapType;
          FieldSchema key = type.getNestedFields(0);
          FieldSchema value = type.getNestedFields(1);
          if (value.getOptional()) {
            // if the value is optional
            mapType = Types.MapType.ofOptional(key.getId(), value.getId(),
                fromProto(key.getType()), fromProto(value.getType()));
          } else {
            mapType = Types.MapType.ofRequired(key.getId(), value.getId(),
                fromProto(key.getType()), fromProto(value.getType()));
          }
          return mapType;
        default:
          return Types.StructType.of(Collections.emptyList());
      }
    }
  }

  private static Types.NestedField fromProto(FieldSchema field) {
    Types.NestedField icebergField;
    if (field.getOptional()) {
      icebergField = Types.NestedField.optional(field.getId(), field.getName(),
          fromProto(field.getType()));
    } else {
      icebergField = Types.NestedField.required(field.getId(), field.getName(),
          fromProto(field.getType()));
    }
    return icebergField;
  }

  /**
   * @param schema schema
   * @return the protocol buffer version of schema
   */
  public static org.apache.iceberg.Schema fromProto(Schema schema) {
    return new org.apache.iceberg.Schema(schema.getColsList().stream()
        .map(ProtoUtils::fromProto).collect(Collectors.toList()));
  }

  /**
   * @param partition the partition proto
   * @return true if the partition has the hive layout, false otherwise
   */
  public static boolean isHiveLayout(Partition partition) {
    if (!partition.hasLayout()) {
      return false;
    }
    Layout layout = partition.getLayout();
    // TODO(gpang): use a layout registry
    return Objects.equals(layout.getLayoutType(), "hive");
  }

  /**
   * @param partition the partition proto
   * @return the hive-specific partition proto
   */
  public static PartitionInfo extractHiveLayout(Partition partition)
      throws InvalidProtocolBufferException {
    if (!isHiveLayout(partition)) {
      if (partition.hasLayout()) {
        throw new IllegalStateException(
            "Cannot parse hive-layout. layoutType: " + partition.getLayout().getLayoutType());
      } else {
        throw new IllegalStateException("Cannot parse hive-layout from missing layout");
      }
    }
    Layout layout = partition.getLayout();
    if (!layout.hasLayoutData()) {
      throw new IllegalStateException("Cannot parse hive-layout from empty layout data");
    }
    return PartitionInfo.parseFrom(layout.getLayoutData());
  }
}
