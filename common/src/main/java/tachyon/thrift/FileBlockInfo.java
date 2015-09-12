/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package tachyon.thrift;

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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-9-10")
public class FileBlockInfo implements org.apache.thrift.TBase<FileBlockInfo, FileBlockInfo._Fields>, java.io.Serializable, Cloneable, Comparable<FileBlockInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("FileBlockInfo");

  private static final org.apache.thrift.protocol.TField BLOCK_INFO_FIELD_DESC = new org.apache.thrift.protocol.TField("blockInfo", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField OFFSET_FIELD_DESC = new org.apache.thrift.protocol.TField("offset", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField UNDER_FS_LOCATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("underFsLocations", org.apache.thrift.protocol.TType.LIST, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new FileBlockInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new FileBlockInfoTupleSchemeFactory());
  }

  public BlockInfo blockInfo; // required
  public long offset; // required
  public List<NetAddress> underFsLocations; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BLOCK_INFO((short)1, "blockInfo"),
    OFFSET((short)2, "offset"),
    UNDER_FS_LOCATIONS((short)3, "underFsLocations");

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
        case 1: // BLOCK_INFO
          return BLOCK_INFO;
        case 2: // OFFSET
          return OFFSET;
        case 3: // UNDER_FS_LOCATIONS
          return UNDER_FS_LOCATIONS;
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
  private static final int __OFFSET_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BLOCK_INFO, new org.apache.thrift.meta_data.FieldMetaData("blockInfo", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, BlockInfo.class)));
    tmpMap.put(_Fields.OFFSET, new org.apache.thrift.meta_data.FieldMetaData("offset", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.UNDER_FS_LOCATIONS, new org.apache.thrift.meta_data.FieldMetaData("underFsLocations", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NetAddress.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(FileBlockInfo.class, metaDataMap);
  }

  public FileBlockInfo() {
  }

  public FileBlockInfo(
    BlockInfo blockInfo,
    long offset,
    List<NetAddress> underFsLocations)
  {
    this();
    this.blockInfo = blockInfo;
    this.offset = offset;
    setOffsetIsSet(true);
    this.underFsLocations = underFsLocations;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public FileBlockInfo(FileBlockInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetBlockInfo()) {
      this.blockInfo = new BlockInfo(other.blockInfo);
    }
    this.offset = other.offset;
    if (other.isSetUnderFsLocations()) {
      List<NetAddress> __this__underFsLocations = new ArrayList<NetAddress>(other.underFsLocations.size());
      for (NetAddress other_element : other.underFsLocations) {
        __this__underFsLocations.add(new NetAddress(other_element));
      }
      this.underFsLocations = __this__underFsLocations;
    }
  }

  public FileBlockInfo deepCopy() {
    return new FileBlockInfo(this);
  }

  @Override
  public void clear() {
    this.blockInfo = null;
    setOffsetIsSet(false);
    this.offset = 0;
    this.underFsLocations = null;
  }

  public BlockInfo getBlockInfo() {
    return this.blockInfo;
  }

  public FileBlockInfo setBlockInfo(BlockInfo blockInfo) {
    this.blockInfo = blockInfo;
    return this;
  }

  public void unsetBlockInfo() {
    this.blockInfo = null;
  }

  /** Returns true if field blockInfo is set (has been assigned a value) and false otherwise */
  public boolean isSetBlockInfo() {
    return this.blockInfo != null;
  }

  public void setBlockInfoIsSet(boolean value) {
    if (!value) {
      this.blockInfo = null;
    }
  }

  public long getOffset() {
    return this.offset;
  }

  public FileBlockInfo setOffset(long offset) {
    this.offset = offset;
    setOffsetIsSet(true);
    return this;
  }

  public void unsetOffset() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __OFFSET_ISSET_ID);
  }

  /** Returns true if field offset is set (has been assigned a value) and false otherwise */
  public boolean isSetOffset() {
    return EncodingUtils.testBit(__isset_bitfield, __OFFSET_ISSET_ID);
  }

  public void setOffsetIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __OFFSET_ISSET_ID, value);
  }

  public int getUnderFsLocationsSize() {
    return (this.underFsLocations == null) ? 0 : this.underFsLocations.size();
  }

  public java.util.Iterator<NetAddress> getUnderFsLocationsIterator() {
    return (this.underFsLocations == null) ? null : this.underFsLocations.iterator();
  }

  public void addToUnderFsLocations(NetAddress elem) {
    if (this.underFsLocations == null) {
      this.underFsLocations = new ArrayList<NetAddress>();
    }
    this.underFsLocations.add(elem);
  }

  public List<NetAddress> getUnderFsLocations() {
    return this.underFsLocations;
  }

  public FileBlockInfo setUnderFsLocations(List<NetAddress> underFsLocations) {
    this.underFsLocations = underFsLocations;
    return this;
  }

  public void unsetUnderFsLocations() {
    this.underFsLocations = null;
  }

  /** Returns true if field underFsLocations is set (has been assigned a value) and false otherwise */
  public boolean isSetUnderFsLocations() {
    return this.underFsLocations != null;
  }

  public void setUnderFsLocationsIsSet(boolean value) {
    if (!value) {
      this.underFsLocations = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BLOCK_INFO:
      if (value == null) {
        unsetBlockInfo();
      } else {
        setBlockInfo((BlockInfo)value);
      }
      break;

    case OFFSET:
      if (value == null) {
        unsetOffset();
      } else {
        setOffset((Long)value);
      }
      break;

    case UNDER_FS_LOCATIONS:
      if (value == null) {
        unsetUnderFsLocations();
      } else {
        setUnderFsLocations((List<NetAddress>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BLOCK_INFO:
      return getBlockInfo();

    case OFFSET:
      return Long.valueOf(getOffset());

    case UNDER_FS_LOCATIONS:
      return getUnderFsLocations();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BLOCK_INFO:
      return isSetBlockInfo();
    case OFFSET:
      return isSetOffset();
    case UNDER_FS_LOCATIONS:
      return isSetUnderFsLocations();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof FileBlockInfo)
      return this.equals((FileBlockInfo)that);
    return false;
  }

  public boolean equals(FileBlockInfo that) {
    if (that == null)
      return false;

    boolean this_present_blockInfo = true && this.isSetBlockInfo();
    boolean that_present_blockInfo = true && that.isSetBlockInfo();
    if (this_present_blockInfo || that_present_blockInfo) {
      if (!(this_present_blockInfo && that_present_blockInfo))
        return false;
      if (!this.blockInfo.equals(that.blockInfo))
        return false;
    }

    boolean this_present_offset = true;
    boolean that_present_offset = true;
    if (this_present_offset || that_present_offset) {
      if (!(this_present_offset && that_present_offset))
        return false;
      if (this.offset != that.offset)
        return false;
    }

    boolean this_present_underFsLocations = true && this.isSetUnderFsLocations();
    boolean that_present_underFsLocations = true && that.isSetUnderFsLocations();
    if (this_present_underFsLocations || that_present_underFsLocations) {
      if (!(this_present_underFsLocations && that_present_underFsLocations))
        return false;
      if (!this.underFsLocations.equals(that.underFsLocations))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_blockInfo = true && (isSetBlockInfo());
    list.add(present_blockInfo);
    if (present_blockInfo)
      list.add(blockInfo);

    boolean present_offset = true;
    list.add(present_offset);
    if (present_offset)
      list.add(offset);

    boolean present_underFsLocations = true && (isSetUnderFsLocations());
    list.add(present_underFsLocations);
    if (present_underFsLocations)
      list.add(underFsLocations);

    return list.hashCode();
  }

  @Override
  public int compareTo(FileBlockInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetBlockInfo()).compareTo(other.isSetBlockInfo());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlockInfo()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blockInfo, other.blockInfo);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOffset()).compareTo(other.isSetOffset());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOffset()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.offset, other.offset);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUnderFsLocations()).compareTo(other.isSetUnderFsLocations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUnderFsLocations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.underFsLocations, other.underFsLocations);
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
    StringBuilder sb = new StringBuilder("FileBlockInfo(");
    boolean first = true;

    sb.append("blockInfo:");
    if (this.blockInfo == null) {
      sb.append("null");
    } else {
      sb.append(this.blockInfo);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("offset:");
    sb.append(this.offset);
    first = false;
    if (!first) sb.append(", ");
    sb.append("underFsLocations:");
    if (this.underFsLocations == null) {
      sb.append("null");
    } else {
      sb.append(this.underFsLocations);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (blockInfo != null) {
      blockInfo.validate();
    }
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

  private static class FileBlockInfoStandardSchemeFactory implements SchemeFactory {
    public FileBlockInfoStandardScheme getScheme() {
      return new FileBlockInfoStandardScheme();
    }
  }

  private static class FileBlockInfoStandardScheme extends StandardScheme<FileBlockInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, FileBlockInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BLOCK_INFO
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.blockInfo = new BlockInfo();
              struct.blockInfo.read(iprot);
              struct.setBlockInfoIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // OFFSET
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.offset = iprot.readI64();
              struct.setOffsetIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // UNDER_FS_LOCATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.underFsLocations = new ArrayList<NetAddress>(_list8.size);
                NetAddress _elem9;
                for (int _i10 = 0; _i10 < _list8.size; ++_i10)
                {
                  _elem9 = new NetAddress();
                  _elem9.read(iprot);
                  struct.underFsLocations.add(_elem9);
                }
                iprot.readListEnd();
              }
              struct.setUnderFsLocationsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, FileBlockInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.blockInfo != null) {
        oprot.writeFieldBegin(BLOCK_INFO_FIELD_DESC);
        struct.blockInfo.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(OFFSET_FIELD_DESC);
      oprot.writeI64(struct.offset);
      oprot.writeFieldEnd();
      if (struct.underFsLocations != null) {
        oprot.writeFieldBegin(UNDER_FS_LOCATIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.underFsLocations.size()));
          for (NetAddress _iter11 : struct.underFsLocations)
          {
            _iter11.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class FileBlockInfoTupleSchemeFactory implements SchemeFactory {
    public FileBlockInfoTupleScheme getScheme() {
      return new FileBlockInfoTupleScheme();
    }
  }

  private static class FileBlockInfoTupleScheme extends TupleScheme<FileBlockInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, FileBlockInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBlockInfo()) {
        optionals.set(0);
      }
      if (struct.isSetOffset()) {
        optionals.set(1);
      }
      if (struct.isSetUnderFsLocations()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetBlockInfo()) {
        struct.blockInfo.write(oprot);
      }
      if (struct.isSetOffset()) {
        oprot.writeI64(struct.offset);
      }
      if (struct.isSetUnderFsLocations()) {
        {
          oprot.writeI32(struct.underFsLocations.size());
          for (NetAddress _iter12 : struct.underFsLocations)
          {
            _iter12.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, FileBlockInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.blockInfo = new BlockInfo();
        struct.blockInfo.read(iprot);
        struct.setBlockInfoIsSet(true);
      }
      if (incoming.get(1)) {
        struct.offset = iprot.readI64();
        struct.setOffsetIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list13 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.underFsLocations = new ArrayList<NetAddress>(_list13.size);
          NetAddress _elem14;
          for (int _i15 = 0; _i15 < _list13.size; ++_i15)
          {
            _elem14 = new NetAddress();
            _elem14.read(iprot);
            struct.underFsLocations.add(_elem14);
          }
        }
        struct.setUnderFsLocationsIsSet(true);
      }
    }
  }

}

