/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package alluxio.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-08-03")
public class ListStatusTOptions implements org.apache.thrift.TBase<ListStatusTOptions, ListStatusTOptions._Fields>, java.io.Serializable, Cloneable, Comparable<ListStatusTOptions> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ListStatusTOptions");

  private static final org.apache.thrift.protocol.TField LOAD_DIRECT_CHILDREN_FIELD_DESC = new org.apache.thrift.protocol.TField("loadDirectChildren", org.apache.thrift.protocol.TType.BOOL, (short)1);
  private static final org.apache.thrift.protocol.TField LOAD_METADATA_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("loadMetadataType", org.apache.thrift.protocol.TType.I32, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ListStatusTOptionsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ListStatusTOptionsTupleSchemeFactory());
  }

  private boolean loadDirectChildren; // optional
  private LoadMetadataTType loadMetadataType; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LOAD_DIRECT_CHILDREN((short)1, "loadDirectChildren"),
    /**
     * 
     * @see LoadMetadataTType
     */
    LOAD_METADATA_TYPE((short)2, "loadMetadataType");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // LOAD_DIRECT_CHILDREN
          return LOAD_DIRECT_CHILDREN;
        case 2: // LOAD_METADATA_TYPE
          return LOAD_METADATA_TYPE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __LOADDIRECTCHILDREN_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.LOAD_DIRECT_CHILDREN,_Fields.LOAD_METADATA_TYPE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.LOAD_DIRECT_CHILDREN, new org.apache.thrift.meta_data.FieldMetaData("loadDirectChildren", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.LOAD_METADATA_TYPE, new org.apache.thrift.meta_data.FieldMetaData("loadMetadataType", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, LoadMetadataTType.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ListStatusTOptions.class, metaDataMap);
  }

  public ListStatusTOptions() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ListStatusTOptions(ListStatusTOptions other) {
    __isset_bitfield = other.__isset_bitfield;
    this.loadDirectChildren = other.loadDirectChildren;
    if (other.isSetLoadMetadataType()) {
      this.loadMetadataType = other.loadMetadataType;
    }
  }

  public ListStatusTOptions deepCopy() {
    return new ListStatusTOptions(this);
  }

  @Override
  public void clear() {
    setLoadDirectChildrenIsSet(false);
    this.loadDirectChildren = false;
    this.loadMetadataType = null;
  }

  public boolean isLoadDirectChildren() {
    return this.loadDirectChildren;
  }

  public ListStatusTOptions setLoadDirectChildren(boolean loadDirectChildren) {
    this.loadDirectChildren = loadDirectChildren;
    setLoadDirectChildrenIsSet(true);
    return this;
  }

  public void unsetLoadDirectChildren() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __LOADDIRECTCHILDREN_ISSET_ID);
  }

  /** Returns true if field loadDirectChildren is set (has been assigned a value) and false otherwise */
  public boolean isSetLoadDirectChildren() {
    return EncodingUtils.testBit(__isset_bitfield, __LOADDIRECTCHILDREN_ISSET_ID);
  }

  public void setLoadDirectChildrenIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __LOADDIRECTCHILDREN_ISSET_ID, value);
  }

  /**
   * 
   * @see LoadMetadataTType
   */
  public LoadMetadataTType getLoadMetadataType() {
    return this.loadMetadataType;
  }

  /**
   * 
   * @see LoadMetadataTType
   */
  public ListStatusTOptions setLoadMetadataType(LoadMetadataTType loadMetadataType) {
    this.loadMetadataType = loadMetadataType;
    return this;
  }

  public void unsetLoadMetadataType() {
    this.loadMetadataType = null;
  }

  /** Returns true if field loadMetadataType is set (has been assigned a value) and false otherwise */
  public boolean isSetLoadMetadataType() {
    return this.loadMetadataType != null;
  }

  public void setLoadMetadataTypeIsSet(boolean value) {
    if (!value) {
      this.loadMetadataType = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case LOAD_DIRECT_CHILDREN:
      if (value == null) {
        unsetLoadDirectChildren();
      } else {
        setLoadDirectChildren((Boolean)value);
      }
      break;

    case LOAD_METADATA_TYPE:
      if (value == null) {
        unsetLoadMetadataType();
      } else {
        setLoadMetadataType((LoadMetadataTType)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case LOAD_DIRECT_CHILDREN:
      return isLoadDirectChildren();

    case LOAD_METADATA_TYPE:
      return getLoadMetadataType();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case LOAD_DIRECT_CHILDREN:
      return isSetLoadDirectChildren();
    case LOAD_METADATA_TYPE:
      return isSetLoadMetadataType();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ListStatusTOptions)
      return this.equals((ListStatusTOptions)that);
    return false;
  }

  public boolean equals(ListStatusTOptions that) {
    if (that == null)
      return false;

    boolean this_present_loadDirectChildren = true && this.isSetLoadDirectChildren();
    boolean that_present_loadDirectChildren = true && that.isSetLoadDirectChildren();
    if (this_present_loadDirectChildren || that_present_loadDirectChildren) {
      if (!(this_present_loadDirectChildren && that_present_loadDirectChildren))
        return false;
      if (this.loadDirectChildren != that.loadDirectChildren)
        return false;
    }

    boolean this_present_loadMetadataType = true && this.isSetLoadMetadataType();
    boolean that_present_loadMetadataType = true && that.isSetLoadMetadataType();
    if (this_present_loadMetadataType || that_present_loadMetadataType) {
      if (!(this_present_loadMetadataType && that_present_loadMetadataType))
        return false;
      if (!this.loadMetadataType.equals(that.loadMetadataType))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_loadDirectChildren = true && (isSetLoadDirectChildren());
    list.add(present_loadDirectChildren);
    if (present_loadDirectChildren)
      list.add(loadDirectChildren);

    boolean present_loadMetadataType = true && (isSetLoadMetadataType());
    list.add(present_loadMetadataType);
    if (present_loadMetadataType)
      list.add(loadMetadataType.getValue());

    return list.hashCode();
  }

  @Override
  public int compareTo(ListStatusTOptions other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetLoadDirectChildren()).compareTo(other.isSetLoadDirectChildren());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLoadDirectChildren()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.loadDirectChildren, other.loadDirectChildren);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLoadMetadataType()).compareTo(other.isSetLoadMetadataType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLoadMetadataType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.loadMetadataType, other.loadMetadataType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ListStatusTOptions(");
    boolean first = true;

    if (isSetLoadDirectChildren()) {
      sb.append("loadDirectChildren:");
      sb.append(this.loadDirectChildren);
      first = false;
    }
    if (isSetLoadMetadataType()) {
      if (!first) sb.append(", ");
      sb.append("loadMetadataType:");
      if (this.loadMetadataType == null) {
        sb.append("null");
      } else {
        sb.append(this.loadMetadataType);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ListStatusTOptionsStandardSchemeFactory implements SchemeFactory {
    public ListStatusTOptionsStandardScheme getScheme() {
      return new ListStatusTOptionsStandardScheme();
    }
  }

  private static class ListStatusTOptionsStandardScheme extends StandardScheme<ListStatusTOptions> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ListStatusTOptions struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // LOAD_DIRECT_CHILDREN
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.loadDirectChildren = iprot.readBool();
              struct.setLoadDirectChildrenIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LOAD_METADATA_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.loadMetadataType = alluxio.thrift.LoadMetadataTType.findByValue(iprot.readI32());
              struct.setLoadMetadataTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ListStatusTOptions struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.isSetLoadDirectChildren()) {
        oprot.writeFieldBegin(LOAD_DIRECT_CHILDREN_FIELD_DESC);
        oprot.writeBool(struct.loadDirectChildren);
        oprot.writeFieldEnd();
      }
      if (struct.loadMetadataType != null) {
        if (struct.isSetLoadMetadataType()) {
          oprot.writeFieldBegin(LOAD_METADATA_TYPE_FIELD_DESC);
          oprot.writeI32(struct.loadMetadataType.getValue());
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ListStatusTOptionsTupleSchemeFactory implements SchemeFactory {
    public ListStatusTOptionsTupleScheme getScheme() {
      return new ListStatusTOptionsTupleScheme();
    }
  }

  private static class ListStatusTOptionsTupleScheme extends TupleScheme<ListStatusTOptions> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ListStatusTOptions struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetLoadDirectChildren()) {
        optionals.set(0);
      }
      if (struct.isSetLoadMetadataType()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetLoadDirectChildren()) {
        oprot.writeBool(struct.loadDirectChildren);
      }
      if (struct.isSetLoadMetadataType()) {
        oprot.writeI32(struct.loadMetadataType.getValue());
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ListStatusTOptions struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.loadDirectChildren = iprot.readBool();
        struct.setLoadDirectChildrenIsSet(true);
      }
      if (incoming.get(1)) {
        struct.loadMetadataType = alluxio.thrift.LoadMetadataTType.findByValue(iprot.readI32());
        struct.setLoadMetadataTypeIsSet(true);
      }
    }
  }

}

