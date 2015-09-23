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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-9-23")
public class CheckpointFile implements org.apache.thrift.TBase<CheckpointFile, CheckpointFile._Fields>, java.io.Serializable, Cloneable, Comparable<CheckpointFile> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CheckpointFile");

  private static final org.apache.thrift.protocol.TField M_FILE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("mFileId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField M_BLOCK_IDS_FIELD_DESC = new org.apache.thrift.protocol.TField("mBlockIds", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField M_UNDER_FS_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField("mUnderFsPath", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CheckpointFileStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CheckpointFileTupleSchemeFactory());
  }

  public long mFileId; // required
  public List<Long> mBlockIds; // required
  public String mUnderFsPath; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    M_FILE_ID((short)1, "mFileId"),
    M_BLOCK_IDS((short)2, "mBlockIds"),
    M_UNDER_FS_PATH((short)3, "mUnderFsPath");

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
        case 1: // M_FILE_ID
          return M_FILE_ID;
        case 2: // M_BLOCK_IDS
          return M_BLOCK_IDS;
        case 3: // M_UNDER_FS_PATH
          return M_UNDER_FS_PATH;
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
  private static final int __MFILEID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.M_FILE_ID, new org.apache.thrift.meta_data.FieldMetaData("mFileId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.M_BLOCK_IDS, new org.apache.thrift.meta_data.FieldMetaData("mBlockIds", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.M_UNDER_FS_PATH, new org.apache.thrift.meta_data.FieldMetaData("mUnderFsPath", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CheckpointFile.class, metaDataMap);
  }

  public CheckpointFile() {
  }

  public CheckpointFile(
    long mFileId,
    List<Long> mBlockIds,
    String mUnderFsPath)
  {
    this();
    this.mFileId = mFileId;
    setMFileIdIsSet(true);
    this.mBlockIds = mBlockIds;
    this.mUnderFsPath = mUnderFsPath;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CheckpointFile(CheckpointFile other) {
    __isset_bitfield = other.__isset_bitfield;
    this.mFileId = other.mFileId;
    if (other.isSetMBlockIds()) {
      List<Long> __this__mBlockIds = new ArrayList<Long>(other.mBlockIds);
      this.mBlockIds = __this__mBlockIds;
    }
    if (other.isSetMUnderFsPath()) {
      this.mUnderFsPath = other.mUnderFsPath;
    }
  }

  public CheckpointFile deepCopy() {
    return new CheckpointFile(this);
  }

  @Override
  public void clear() {
    setMFileIdIsSet(false);
    this.mFileId = 0;
    this.mBlockIds = null;
    this.mUnderFsPath = null;
  }

  public long getMFileId() {
    return this.mFileId;
  }

  public CheckpointFile setMFileId(long mFileId) {
    this.mFileId = mFileId;
    setMFileIdIsSet(true);
    return this;
  }

  public void unsetMFileId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MFILEID_ISSET_ID);
  }

  /** Returns true if field mFileId is set (has been assigned a value) and false otherwise */
  public boolean isSetMFileId() {
    return EncodingUtils.testBit(__isset_bitfield, __MFILEID_ISSET_ID);
  }

  public void setMFileIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MFILEID_ISSET_ID, value);
  }

  public int getMBlockIdsSize() {
    return (this.mBlockIds == null) ? 0 : this.mBlockIds.size();
  }

  public java.util.Iterator<Long> getMBlockIdsIterator() {
    return (this.mBlockIds == null) ? null : this.mBlockIds.iterator();
  }

  public void addToMBlockIds(long elem) {
    if (this.mBlockIds == null) {
      this.mBlockIds = new ArrayList<Long>();
    }
    this.mBlockIds.add(elem);
  }

  public List<Long> getMBlockIds() {
    return this.mBlockIds;
  }

  public CheckpointFile setMBlockIds(List<Long> mBlockIds) {
    this.mBlockIds = mBlockIds;
    return this;
  }

  public void unsetMBlockIds() {
    this.mBlockIds = null;
  }

  /** Returns true if field mBlockIds is set (has been assigned a value) and false otherwise */
  public boolean isSetMBlockIds() {
    return this.mBlockIds != null;
  }

  public void setMBlockIdsIsSet(boolean value) {
    if (!value) {
      this.mBlockIds = null;
    }
  }

  public String getMUnderFsPath() {
    return this.mUnderFsPath;
  }

  public CheckpointFile setMUnderFsPath(String mUnderFsPath) {
    this.mUnderFsPath = mUnderFsPath;
    return this;
  }

  public void unsetMUnderFsPath() {
    this.mUnderFsPath = null;
  }

  /** Returns true if field mUnderFsPath is set (has been assigned a value) and false otherwise */
  public boolean isSetMUnderFsPath() {
    return this.mUnderFsPath != null;
  }

  public void setMUnderFsPathIsSet(boolean value) {
    if (!value) {
      this.mUnderFsPath = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case M_FILE_ID:
      if (value == null) {
        unsetMFileId();
      } else {
        setMFileId((Long)value);
      }
      break;

    case M_BLOCK_IDS:
      if (value == null) {
        unsetMBlockIds();
      } else {
        setMBlockIds((List<Long>)value);
      }
      break;

    case M_UNDER_FS_PATH:
      if (value == null) {
        unsetMUnderFsPath();
      } else {
        setMUnderFsPath((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case M_FILE_ID:
      return Long.valueOf(getMFileId());

    case M_BLOCK_IDS:
      return getMBlockIds();

    case M_UNDER_FS_PATH:
      return getMUnderFsPath();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case M_FILE_ID:
      return isSetMFileId();
    case M_BLOCK_IDS:
      return isSetMBlockIds();
    case M_UNDER_FS_PATH:
      return isSetMUnderFsPath();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CheckpointFile)
      return this.equals((CheckpointFile)that);
    return false;
  }

  public boolean equals(CheckpointFile that) {
    if (that == null)
      return false;

    boolean this_present_mFileId = true;
    boolean that_present_mFileId = true;
    if (this_present_mFileId || that_present_mFileId) {
      if (!(this_present_mFileId && that_present_mFileId))
        return false;
      if (this.mFileId != that.mFileId)
        return false;
    }

    boolean this_present_mBlockIds = true && this.isSetMBlockIds();
    boolean that_present_mBlockIds = true && that.isSetMBlockIds();
    if (this_present_mBlockIds || that_present_mBlockIds) {
      if (!(this_present_mBlockIds && that_present_mBlockIds))
        return false;
      if (!this.mBlockIds.equals(that.mBlockIds))
        return false;
    }

    boolean this_present_mUnderFsPath = true && this.isSetMUnderFsPath();
    boolean that_present_mUnderFsPath = true && that.isSetMUnderFsPath();
    if (this_present_mUnderFsPath || that_present_mUnderFsPath) {
      if (!(this_present_mUnderFsPath && that_present_mUnderFsPath))
        return false;
      if (!this.mUnderFsPath.equals(that.mUnderFsPath))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_mFileId = true;
    list.add(present_mFileId);
    if (present_mFileId)
      list.add(mFileId);

    boolean present_mBlockIds = true && (isSetMBlockIds());
    list.add(present_mBlockIds);
    if (present_mBlockIds)
      list.add(mBlockIds);

    boolean present_mUnderFsPath = true && (isSetMUnderFsPath());
    list.add(present_mUnderFsPath);
    if (present_mUnderFsPath)
      list.add(mUnderFsPath);

    return list.hashCode();
  }

  @Override
  public int compareTo(CheckpointFile other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetMFileId()).compareTo(other.isSetMFileId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMFileId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.mFileId, other.mFileId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMBlockIds()).compareTo(other.isSetMBlockIds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMBlockIds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.mBlockIds, other.mBlockIds);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMUnderFsPath()).compareTo(other.isSetMUnderFsPath());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMUnderFsPath()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.mUnderFsPath, other.mUnderFsPath);
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
    StringBuilder sb = new StringBuilder("CheckpointFile(");
    boolean first = true;

    sb.append("mFileId:");
    sb.append(this.mFileId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("mBlockIds:");
    if (this.mBlockIds == null) {
      sb.append("null");
    } else {
      sb.append(this.mBlockIds);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("mUnderFsPath:");
    if (this.mUnderFsPath == null) {
      sb.append("null");
    } else {
      sb.append(this.mUnderFsPath);
    }
    first = false;
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

  private static class CheckpointFileStandardSchemeFactory implements SchemeFactory {
    public CheckpointFileStandardScheme getScheme() {
      return new CheckpointFileStandardScheme();
    }
  }

  private static class CheckpointFileStandardScheme extends StandardScheme<CheckpointFile> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CheckpointFile struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // M_FILE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.mFileId = iprot.readI64();
              struct.setMFileIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // M_BLOCK_IDS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list64 = iprot.readListBegin();
                struct.mBlockIds = new ArrayList<Long>(_list64.size);
                long _elem65;
                for (int _i66 = 0; _i66 < _list64.size; ++_i66)
                {
                  _elem65 = iprot.readI64();
                  struct.mBlockIds.add(_elem65);
                }
                iprot.readListEnd();
              }
              struct.setMBlockIdsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // M_UNDER_FS_PATH
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.mUnderFsPath = iprot.readString();
              struct.setMUnderFsPathIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, CheckpointFile struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(M_FILE_ID_FIELD_DESC);
      oprot.writeI64(struct.mFileId);
      oprot.writeFieldEnd();
      if (struct.mBlockIds != null) {
        oprot.writeFieldBegin(M_BLOCK_IDS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, struct.mBlockIds.size()));
          for (long _iter67 : struct.mBlockIds)
          {
            oprot.writeI64(_iter67);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.mUnderFsPath != null) {
        oprot.writeFieldBegin(M_UNDER_FS_PATH_FIELD_DESC);
        oprot.writeString(struct.mUnderFsPath);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CheckpointFileTupleSchemeFactory implements SchemeFactory {
    public CheckpointFileTupleScheme getScheme() {
      return new CheckpointFileTupleScheme();
    }
  }

  private static class CheckpointFileTupleScheme extends TupleScheme<CheckpointFile> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CheckpointFile struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetMFileId()) {
        optionals.set(0);
      }
      if (struct.isSetMBlockIds()) {
        optionals.set(1);
      }
      if (struct.isSetMUnderFsPath()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetMFileId()) {
        oprot.writeI64(struct.mFileId);
      }
      if (struct.isSetMBlockIds()) {
        {
          oprot.writeI32(struct.mBlockIds.size());
          for (long _iter68 : struct.mBlockIds)
          {
            oprot.writeI64(_iter68);
          }
        }
      }
      if (struct.isSetMUnderFsPath()) {
        oprot.writeString(struct.mUnderFsPath);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CheckpointFile struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.mFileId = iprot.readI64();
        struct.setMFileIdIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list69 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.mBlockIds = new ArrayList<Long>(_list69.size);
          long _elem70;
          for (int _i71 = 0; _i71 < _list69.size; ++_i71)
          {
            _elem70 = iprot.readI64();
            struct.mBlockIds.add(_elem70);
          }
        }
        struct.setMBlockIdsIsSet(true);
      }
      if (incoming.get(2)) {
        struct.mUnderFsPath = iprot.readString();
        struct.setMUnderFsPathIsSet(true);
      }
    }
  }

}

