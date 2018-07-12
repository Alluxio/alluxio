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
/**
 * Contains the information of a block in a file. In addition to the BlockInfo, it includes the
 * offset in the file, and the under file system locations of the block replicas.
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class FileBlockInfo implements org.apache.thrift.TBase<FileBlockInfo, FileBlockInfo._Fields>, java.io.Serializable, Cloneable, Comparable<FileBlockInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("FileBlockInfo");

  private static final org.apache.thrift.protocol.TField BLOCK_INFO_FIELD_DESC = new org.apache.thrift.protocol.TField("blockInfo", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField OFFSET_FIELD_DESC = new org.apache.thrift.protocol.TField("offset", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField UFS_LOCATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("ufsLocations", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField UFS_STRING_LOCATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("ufsStringLocations", org.apache.thrift.protocol.TType.LIST, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new FileBlockInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new FileBlockInfoTupleSchemeFactory());
  }

  private alluxio.thrift.BlockInfo blockInfo; // required
  private long offset; // required
  private List<alluxio.thrift.WorkerNetAddress> ufsLocations; // required
  private List<String> ufsStringLocations; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BLOCK_INFO((short)1, "blockInfo"),
    OFFSET((short)2, "offset"),
    UFS_LOCATIONS((short)3, "ufsLocations"),
    UFS_STRING_LOCATIONS((short)4, "ufsStringLocations");

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
        case 3: // UFS_LOCATIONS
          return UFS_LOCATIONS;
        case 4: // UFS_STRING_LOCATIONS
          return UFS_STRING_LOCATIONS;
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
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, alluxio.thrift.BlockInfo.class)));
    tmpMap.put(_Fields.OFFSET, new org.apache.thrift.meta_data.FieldMetaData("offset", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.UFS_LOCATIONS, new org.apache.thrift.meta_data.FieldMetaData("ufsLocations", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, alluxio.thrift.WorkerNetAddress.class))));
    tmpMap.put(_Fields.UFS_STRING_LOCATIONS, new org.apache.thrift.meta_data.FieldMetaData("ufsStringLocations", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(FileBlockInfo.class, metaDataMap);
  }

  public FileBlockInfo() {
  }

  public FileBlockInfo(
    alluxio.thrift.BlockInfo blockInfo,
    long offset,
    List<alluxio.thrift.WorkerNetAddress> ufsLocations,
    List<String> ufsStringLocations)
  {
    this();
    this.blockInfo = blockInfo;
    this.offset = offset;
    setOffsetIsSet(true);
    this.ufsLocations = ufsLocations;
    this.ufsStringLocations = ufsStringLocations;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public FileBlockInfo(FileBlockInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetBlockInfo()) {
      this.blockInfo = new alluxio.thrift.BlockInfo(other.blockInfo);
    }
    this.offset = other.offset;
    if (other.isSetUfsLocations()) {
      List<alluxio.thrift.WorkerNetAddress> __this__ufsLocations = new ArrayList<alluxio.thrift.WorkerNetAddress>(other.ufsLocations.size());
      for (alluxio.thrift.WorkerNetAddress other_element : other.ufsLocations) {
        __this__ufsLocations.add(new alluxio.thrift.WorkerNetAddress(other_element));
      }
      this.ufsLocations = __this__ufsLocations;
    }
    if (other.isSetUfsStringLocations()) {
      List<String> __this__ufsStringLocations = new ArrayList<String>(other.ufsStringLocations);
      this.ufsStringLocations = __this__ufsStringLocations;
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
    this.ufsLocations = null;
    this.ufsStringLocations = null;
  }

  public alluxio.thrift.BlockInfo getBlockInfo() {
    return this.blockInfo;
  }

  public FileBlockInfo setBlockInfo(alluxio.thrift.BlockInfo blockInfo) {
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

  public int getUfsLocationsSize() {
    return (this.ufsLocations == null) ? 0 : this.ufsLocations.size();
  }

  public java.util.Iterator<alluxio.thrift.WorkerNetAddress> getUfsLocationsIterator() {
    return (this.ufsLocations == null) ? null : this.ufsLocations.iterator();
  }

  public void addToUfsLocations(alluxio.thrift.WorkerNetAddress elem) {
    if (this.ufsLocations == null) {
      this.ufsLocations = new ArrayList<alluxio.thrift.WorkerNetAddress>();
    }
    this.ufsLocations.add(elem);
  }

  public List<alluxio.thrift.WorkerNetAddress> getUfsLocations() {
    return this.ufsLocations;
  }

  public FileBlockInfo setUfsLocations(List<alluxio.thrift.WorkerNetAddress> ufsLocations) {
    this.ufsLocations = ufsLocations;
    return this;
  }

  public void unsetUfsLocations() {
    this.ufsLocations = null;
  }

  /** Returns true if field ufsLocations is set (has been assigned a value) and false otherwise */
  public boolean isSetUfsLocations() {
    return this.ufsLocations != null;
  }

  public void setUfsLocationsIsSet(boolean value) {
    if (!value) {
      this.ufsLocations = null;
    }
  }

  public int getUfsStringLocationsSize() {
    return (this.ufsStringLocations == null) ? 0 : this.ufsStringLocations.size();
  }

  public java.util.Iterator<String> getUfsStringLocationsIterator() {
    return (this.ufsStringLocations == null) ? null : this.ufsStringLocations.iterator();
  }

  public void addToUfsStringLocations(String elem) {
    if (this.ufsStringLocations == null) {
      this.ufsStringLocations = new ArrayList<String>();
    }
    this.ufsStringLocations.add(elem);
  }

  public List<String> getUfsStringLocations() {
    return this.ufsStringLocations;
  }

  public FileBlockInfo setUfsStringLocations(List<String> ufsStringLocations) {
    this.ufsStringLocations = ufsStringLocations;
    return this;
  }

  public void unsetUfsStringLocations() {
    this.ufsStringLocations = null;
  }

  /** Returns true if field ufsStringLocations is set (has been assigned a value) and false otherwise */
  public boolean isSetUfsStringLocations() {
    return this.ufsStringLocations != null;
  }

  public void setUfsStringLocationsIsSet(boolean value) {
    if (!value) {
      this.ufsStringLocations = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BLOCK_INFO:
      if (value == null) {
        unsetBlockInfo();
      } else {
        setBlockInfo((alluxio.thrift.BlockInfo)value);
      }
      break;

    case OFFSET:
      if (value == null) {
        unsetOffset();
      } else {
        setOffset((Long)value);
      }
      break;

    case UFS_LOCATIONS:
      if (value == null) {
        unsetUfsLocations();
      } else {
        setUfsLocations((List<alluxio.thrift.WorkerNetAddress>)value);
      }
      break;

    case UFS_STRING_LOCATIONS:
      if (value == null) {
        unsetUfsStringLocations();
      } else {
        setUfsStringLocations((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BLOCK_INFO:
      return getBlockInfo();

    case OFFSET:
      return getOffset();

    case UFS_LOCATIONS:
      return getUfsLocations();

    case UFS_STRING_LOCATIONS:
      return getUfsStringLocations();

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
    case UFS_LOCATIONS:
      return isSetUfsLocations();
    case UFS_STRING_LOCATIONS:
      return isSetUfsStringLocations();
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

    boolean this_present_ufsLocations = true && this.isSetUfsLocations();
    boolean that_present_ufsLocations = true && that.isSetUfsLocations();
    if (this_present_ufsLocations || that_present_ufsLocations) {
      if (!(this_present_ufsLocations && that_present_ufsLocations))
        return false;
      if (!this.ufsLocations.equals(that.ufsLocations))
        return false;
    }

    boolean this_present_ufsStringLocations = true && this.isSetUfsStringLocations();
    boolean that_present_ufsStringLocations = true && that.isSetUfsStringLocations();
    if (this_present_ufsStringLocations || that_present_ufsStringLocations) {
      if (!(this_present_ufsStringLocations && that_present_ufsStringLocations))
        return false;
      if (!this.ufsStringLocations.equals(that.ufsStringLocations))
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

    boolean present_ufsLocations = true && (isSetUfsLocations());
    list.add(present_ufsLocations);
    if (present_ufsLocations)
      list.add(ufsLocations);

    boolean present_ufsStringLocations = true && (isSetUfsStringLocations());
    list.add(present_ufsStringLocations);
    if (present_ufsStringLocations)
      list.add(ufsStringLocations);

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
    lastComparison = Boolean.valueOf(isSetUfsLocations()).compareTo(other.isSetUfsLocations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUfsLocations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ufsLocations, other.ufsLocations);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUfsStringLocations()).compareTo(other.isSetUfsStringLocations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUfsStringLocations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ufsStringLocations, other.ufsStringLocations);
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
    sb.append("ufsLocations:");
    if (this.ufsLocations == null) {
      sb.append("null");
    } else {
      sb.append(this.ufsLocations);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("ufsStringLocations:");
    if (this.ufsStringLocations == null) {
      sb.append("null");
    } else {
      sb.append(this.ufsStringLocations);
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
              struct.blockInfo = new alluxio.thrift.BlockInfo();
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
          case 3: // UFS_LOCATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list32 = iprot.readListBegin();
                struct.ufsLocations = new ArrayList<alluxio.thrift.WorkerNetAddress>(_list32.size);
                alluxio.thrift.WorkerNetAddress _elem33;
                for (int _i34 = 0; _i34 < _list32.size; ++_i34)
                {
                  _elem33 = new alluxio.thrift.WorkerNetAddress();
                  _elem33.read(iprot);
                  struct.ufsLocations.add(_elem33);
                }
                iprot.readListEnd();
              }
              struct.setUfsLocationsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // UFS_STRING_LOCATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list35 = iprot.readListBegin();
                struct.ufsStringLocations = new ArrayList<String>(_list35.size);
                String _elem36;
                for (int _i37 = 0; _i37 < _list35.size; ++_i37)
                {
                  _elem36 = iprot.readString();
                  struct.ufsStringLocations.add(_elem36);
                }
                iprot.readListEnd();
              }
              struct.setUfsStringLocationsIsSet(true);
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
      if (struct.ufsLocations != null) {
        oprot.writeFieldBegin(UFS_LOCATIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.ufsLocations.size()));
          for (alluxio.thrift.WorkerNetAddress _iter38 : struct.ufsLocations)
          {
            _iter38.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.ufsStringLocations != null) {
        oprot.writeFieldBegin(UFS_STRING_LOCATIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.ufsStringLocations.size()));
          for (String _iter39 : struct.ufsStringLocations)
          {
            oprot.writeString(_iter39);
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
      if (struct.isSetUfsLocations()) {
        optionals.set(2);
      }
      if (struct.isSetUfsStringLocations()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetBlockInfo()) {
        struct.blockInfo.write(oprot);
      }
      if (struct.isSetOffset()) {
        oprot.writeI64(struct.offset);
      }
      if (struct.isSetUfsLocations()) {
        {
          oprot.writeI32(struct.ufsLocations.size());
          for (alluxio.thrift.WorkerNetAddress _iter40 : struct.ufsLocations)
          {
            _iter40.write(oprot);
          }
        }
      }
      if (struct.isSetUfsStringLocations()) {
        {
          oprot.writeI32(struct.ufsStringLocations.size());
          for (String _iter41 : struct.ufsStringLocations)
          {
            oprot.writeString(_iter41);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, FileBlockInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.blockInfo = new alluxio.thrift.BlockInfo();
        struct.blockInfo.read(iprot);
        struct.setBlockInfoIsSet(true);
      }
      if (incoming.get(1)) {
        struct.offset = iprot.readI64();
        struct.setOffsetIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list42 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.ufsLocations = new ArrayList<alluxio.thrift.WorkerNetAddress>(_list42.size);
          alluxio.thrift.WorkerNetAddress _elem43;
          for (int _i44 = 0; _i44 < _list42.size; ++_i44)
          {
            _elem43 = new alluxio.thrift.WorkerNetAddress();
            _elem43.read(iprot);
            struct.ufsLocations.add(_elem43);
          }
        }
        struct.setUfsLocationsIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list45 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.ufsStringLocations = new ArrayList<String>(_list45.size);
          String _elem46;
          for (int _i47 = 0; _i47 < _list45.size; ++_i47)
          {
            _elem46 = iprot.readString();
            struct.ufsStringLocations.add(_elem46);
          }
        }
        struct.setUfsStringLocationsIsSet(true);
      }
    }
  }

}

