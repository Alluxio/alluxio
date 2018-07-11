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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class CreateFileTOptions implements org.apache.thrift.TBase<CreateFileTOptions, CreateFileTOptions._Fields>, java.io.Serializable, Cloneable, Comparable<CreateFileTOptions> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CreateFileTOptions");

  private static final org.apache.thrift.protocol.TField BLOCK_SIZE_BYTES_FIELD_DESC = new org.apache.thrift.protocol.TField("blockSizeBytes", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField PERSISTED_FIELD_DESC = new org.apache.thrift.protocol.TField("persisted", org.apache.thrift.protocol.TType.BOOL, (short)2);
  private static final org.apache.thrift.protocol.TField RECURSIVE_FIELD_DESC = new org.apache.thrift.protocol.TField("recursive", org.apache.thrift.protocol.TType.BOOL, (short)3);
  private static final org.apache.thrift.protocol.TField TTL_NOT_USED_FIELD_DESC = new org.apache.thrift.protocol.TField("ttlNotUsed", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField MODE_FIELD_DESC = new org.apache.thrift.protocol.TField("mode", org.apache.thrift.protocol.TType.I16, (short)5);
  private static final org.apache.thrift.protocol.TField TTL_ACTION_NOT_USED_FIELD_DESC = new org.apache.thrift.protocol.TField("ttlActionNotUsed", org.apache.thrift.protocol.TType.I32, (short)6);
  private static final org.apache.thrift.protocol.TField COMMON_OPTIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("commonOptions", org.apache.thrift.protocol.TType.STRUCT, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CreateFileTOptionsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CreateFileTOptionsTupleSchemeFactory());
  }

  private long blockSizeBytes; // optional
  private boolean persisted; // optional
  private boolean recursive; // optional
  private long ttlNotUsed; // optional
  private short mode; // optional
  private alluxio.thrift.TTtlAction ttlActionNotUsed; // optional
  private FileSystemMasterCommonTOptions commonOptions; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BLOCK_SIZE_BYTES((short)1, "blockSizeBytes"),
    PERSISTED((short)2, "persisted"),
    RECURSIVE((short)3, "recursive"),
    TTL_NOT_USED((short)4, "ttlNotUsed"),
    MODE((short)5, "mode"),
    /**
     * 
     * @see alluxio.thrift.TTtlAction
     */
    TTL_ACTION_NOT_USED((short)6, "ttlActionNotUsed"),
    COMMON_OPTIONS((short)7, "commonOptions");

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
        case 1: // BLOCK_SIZE_BYTES
          return BLOCK_SIZE_BYTES;
        case 2: // PERSISTED
          return PERSISTED;
        case 3: // RECURSIVE
          return RECURSIVE;
        case 4: // TTL_NOT_USED
          return TTL_NOT_USED;
        case 5: // MODE
          return MODE;
        case 6: // TTL_ACTION_NOT_USED
          return TTL_ACTION_NOT_USED;
        case 7: // COMMON_OPTIONS
          return COMMON_OPTIONS;
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
  private static final int __BLOCKSIZEBYTES_ISSET_ID = 0;
  private static final int __PERSISTED_ISSET_ID = 1;
  private static final int __RECURSIVE_ISSET_ID = 2;
  private static final int __TTLNOTUSED_ISSET_ID = 3;
  private static final int __MODE_ISSET_ID = 4;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.BLOCK_SIZE_BYTES,_Fields.PERSISTED,_Fields.RECURSIVE,_Fields.TTL_NOT_USED,_Fields.MODE,_Fields.TTL_ACTION_NOT_USED,_Fields.COMMON_OPTIONS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BLOCK_SIZE_BYTES, new org.apache.thrift.meta_data.FieldMetaData("blockSizeBytes", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.PERSISTED, new org.apache.thrift.meta_data.FieldMetaData("persisted", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.RECURSIVE, new org.apache.thrift.meta_data.FieldMetaData("recursive", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.TTL_NOT_USED, new org.apache.thrift.meta_data.FieldMetaData("ttlNotUsed", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.MODE, new org.apache.thrift.meta_data.FieldMetaData("mode", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I16)));
    tmpMap.put(_Fields.TTL_ACTION_NOT_USED, new org.apache.thrift.meta_data.FieldMetaData("ttlActionNotUsed", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, alluxio.thrift.TTtlAction.class)));
    tmpMap.put(_Fields.COMMON_OPTIONS, new org.apache.thrift.meta_data.FieldMetaData("commonOptions", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, FileSystemMasterCommonTOptions.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CreateFileTOptions.class, metaDataMap);
  }

  public CreateFileTOptions() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CreateFileTOptions(CreateFileTOptions other) {
    __isset_bitfield = other.__isset_bitfield;
    this.blockSizeBytes = other.blockSizeBytes;
    this.persisted = other.persisted;
    this.recursive = other.recursive;
    this.ttlNotUsed = other.ttlNotUsed;
    this.mode = other.mode;
    if (other.isSetTtlActionNotUsed()) {
      this.ttlActionNotUsed = other.ttlActionNotUsed;
    }
    if (other.isSetCommonOptions()) {
      this.commonOptions = new FileSystemMasterCommonTOptions(other.commonOptions);
    }
  }

  public CreateFileTOptions deepCopy() {
    return new CreateFileTOptions(this);
  }

  @Override
  public void clear() {
    setBlockSizeBytesIsSet(false);
    this.blockSizeBytes = 0;
    setPersistedIsSet(false);
    this.persisted = false;
    setRecursiveIsSet(false);
    this.recursive = false;
    setTtlNotUsedIsSet(false);
    this.ttlNotUsed = 0;
    setModeIsSet(false);
    this.mode = 0;
    this.ttlActionNotUsed = null;
    this.commonOptions = null;
  }

  public long getBlockSizeBytes() {
    return this.blockSizeBytes;
  }

  public CreateFileTOptions setBlockSizeBytes(long blockSizeBytes) {
    this.blockSizeBytes = blockSizeBytes;
    setBlockSizeBytesIsSet(true);
    return this;
  }

  public void unsetBlockSizeBytes() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __BLOCKSIZEBYTES_ISSET_ID);
  }

  /** Returns true if field blockSizeBytes is set (has been assigned a value) and false otherwise */
  public boolean isSetBlockSizeBytes() {
    return EncodingUtils.testBit(__isset_bitfield, __BLOCKSIZEBYTES_ISSET_ID);
  }

  public void setBlockSizeBytesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __BLOCKSIZEBYTES_ISSET_ID, value);
  }

  public boolean isPersisted() {
    return this.persisted;
  }

  public CreateFileTOptions setPersisted(boolean persisted) {
    this.persisted = persisted;
    setPersistedIsSet(true);
    return this;
  }

  public void unsetPersisted() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PERSISTED_ISSET_ID);
  }

  /** Returns true if field persisted is set (has been assigned a value) and false otherwise */
  public boolean isSetPersisted() {
    return EncodingUtils.testBit(__isset_bitfield, __PERSISTED_ISSET_ID);
  }

  public void setPersistedIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PERSISTED_ISSET_ID, value);
  }

  public boolean isRecursive() {
    return this.recursive;
  }

  public CreateFileTOptions setRecursive(boolean recursive) {
    this.recursive = recursive;
    setRecursiveIsSet(true);
    return this;
  }

  public void unsetRecursive() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __RECURSIVE_ISSET_ID);
  }

  /** Returns true if field recursive is set (has been assigned a value) and false otherwise */
  public boolean isSetRecursive() {
    return EncodingUtils.testBit(__isset_bitfield, __RECURSIVE_ISSET_ID);
  }

  public void setRecursiveIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __RECURSIVE_ISSET_ID, value);
  }

  public long getTtlNotUsed() {
    return this.ttlNotUsed;
  }

  public CreateFileTOptions setTtlNotUsed(long ttlNotUsed) {
    this.ttlNotUsed = ttlNotUsed;
    setTtlNotUsedIsSet(true);
    return this;
  }

  public void unsetTtlNotUsed() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TTLNOTUSED_ISSET_ID);
  }

  /** Returns true if field ttlNotUsed is set (has been assigned a value) and false otherwise */
  public boolean isSetTtlNotUsed() {
    return EncodingUtils.testBit(__isset_bitfield, __TTLNOTUSED_ISSET_ID);
  }

  public void setTtlNotUsedIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TTLNOTUSED_ISSET_ID, value);
  }

  public short getMode() {
    return this.mode;
  }

  public CreateFileTOptions setMode(short mode) {
    this.mode = mode;
    setModeIsSet(true);
    return this;
  }

  public void unsetMode() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MODE_ISSET_ID);
  }

  /** Returns true if field mode is set (has been assigned a value) and false otherwise */
  public boolean isSetMode() {
    return EncodingUtils.testBit(__isset_bitfield, __MODE_ISSET_ID);
  }

  public void setModeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MODE_ISSET_ID, value);
  }

  /**
   * 
   * @see alluxio.thrift.TTtlAction
   */
  public alluxio.thrift.TTtlAction getTtlActionNotUsed() {
    return this.ttlActionNotUsed;
  }

  /**
   * 
   * @see alluxio.thrift.TTtlAction
   */
  public CreateFileTOptions setTtlActionNotUsed(alluxio.thrift.TTtlAction ttlActionNotUsed) {
    this.ttlActionNotUsed = ttlActionNotUsed;
    return this;
  }

  public void unsetTtlActionNotUsed() {
    this.ttlActionNotUsed = null;
  }

  /** Returns true if field ttlActionNotUsed is set (has been assigned a value) and false otherwise */
  public boolean isSetTtlActionNotUsed() {
    return this.ttlActionNotUsed != null;
  }

  public void setTtlActionNotUsedIsSet(boolean value) {
    if (!value) {
      this.ttlActionNotUsed = null;
    }
  }

  public FileSystemMasterCommonTOptions getCommonOptions() {
    return this.commonOptions;
  }

  public CreateFileTOptions setCommonOptions(FileSystemMasterCommonTOptions commonOptions) {
    this.commonOptions = commonOptions;
    return this;
  }

  public void unsetCommonOptions() {
    this.commonOptions = null;
  }

  /** Returns true if field commonOptions is set (has been assigned a value) and false otherwise */
  public boolean isSetCommonOptions() {
    return this.commonOptions != null;
  }

  public void setCommonOptionsIsSet(boolean value) {
    if (!value) {
      this.commonOptions = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BLOCK_SIZE_BYTES:
      if (value == null) {
        unsetBlockSizeBytes();
      } else {
        setBlockSizeBytes((Long)value);
      }
      break;

    case PERSISTED:
      if (value == null) {
        unsetPersisted();
      } else {
        setPersisted((Boolean)value);
      }
      break;

    case RECURSIVE:
      if (value == null) {
        unsetRecursive();
      } else {
        setRecursive((Boolean)value);
      }
      break;

    case TTL_NOT_USED:
      if (value == null) {
        unsetTtlNotUsed();
      } else {
        setTtlNotUsed((Long)value);
      }
      break;

    case MODE:
      if (value == null) {
        unsetMode();
      } else {
        setMode((Short)value);
      }
      break;

    case TTL_ACTION_NOT_USED:
      if (value == null) {
        unsetTtlActionNotUsed();
      } else {
        setTtlActionNotUsed((alluxio.thrift.TTtlAction)value);
      }
      break;

    case COMMON_OPTIONS:
      if (value == null) {
        unsetCommonOptions();
      } else {
        setCommonOptions((FileSystemMasterCommonTOptions)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BLOCK_SIZE_BYTES:
      return getBlockSizeBytes();

    case PERSISTED:
      return isPersisted();

    case RECURSIVE:
      return isRecursive();

    case TTL_NOT_USED:
      return getTtlNotUsed();

    case MODE:
      return getMode();

    case TTL_ACTION_NOT_USED:
      return getTtlActionNotUsed();

    case COMMON_OPTIONS:
      return getCommonOptions();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BLOCK_SIZE_BYTES:
      return isSetBlockSizeBytes();
    case PERSISTED:
      return isSetPersisted();
    case RECURSIVE:
      return isSetRecursive();
    case TTL_NOT_USED:
      return isSetTtlNotUsed();
    case MODE:
      return isSetMode();
    case TTL_ACTION_NOT_USED:
      return isSetTtlActionNotUsed();
    case COMMON_OPTIONS:
      return isSetCommonOptions();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CreateFileTOptions)
      return this.equals((CreateFileTOptions)that);
    return false;
  }

  public boolean equals(CreateFileTOptions that) {
    if (that == null)
      return false;

    boolean this_present_blockSizeBytes = true && this.isSetBlockSizeBytes();
    boolean that_present_blockSizeBytes = true && that.isSetBlockSizeBytes();
    if (this_present_blockSizeBytes || that_present_blockSizeBytes) {
      if (!(this_present_blockSizeBytes && that_present_blockSizeBytes))
        return false;
      if (this.blockSizeBytes != that.blockSizeBytes)
        return false;
    }

    boolean this_present_persisted = true && this.isSetPersisted();
    boolean that_present_persisted = true && that.isSetPersisted();
    if (this_present_persisted || that_present_persisted) {
      if (!(this_present_persisted && that_present_persisted))
        return false;
      if (this.persisted != that.persisted)
        return false;
    }

    boolean this_present_recursive = true && this.isSetRecursive();
    boolean that_present_recursive = true && that.isSetRecursive();
    if (this_present_recursive || that_present_recursive) {
      if (!(this_present_recursive && that_present_recursive))
        return false;
      if (this.recursive != that.recursive)
        return false;
    }

    boolean this_present_ttlNotUsed = true && this.isSetTtlNotUsed();
    boolean that_present_ttlNotUsed = true && that.isSetTtlNotUsed();
    if (this_present_ttlNotUsed || that_present_ttlNotUsed) {
      if (!(this_present_ttlNotUsed && that_present_ttlNotUsed))
        return false;
      if (this.ttlNotUsed != that.ttlNotUsed)
        return false;
    }

    boolean this_present_mode = true && this.isSetMode();
    boolean that_present_mode = true && that.isSetMode();
    if (this_present_mode || that_present_mode) {
      if (!(this_present_mode && that_present_mode))
        return false;
      if (this.mode != that.mode)
        return false;
    }

    boolean this_present_ttlActionNotUsed = true && this.isSetTtlActionNotUsed();
    boolean that_present_ttlActionNotUsed = true && that.isSetTtlActionNotUsed();
    if (this_present_ttlActionNotUsed || that_present_ttlActionNotUsed) {
      if (!(this_present_ttlActionNotUsed && that_present_ttlActionNotUsed))
        return false;
      if (!this.ttlActionNotUsed.equals(that.ttlActionNotUsed))
        return false;
    }

    boolean this_present_commonOptions = true && this.isSetCommonOptions();
    boolean that_present_commonOptions = true && that.isSetCommonOptions();
    if (this_present_commonOptions || that_present_commonOptions) {
      if (!(this_present_commonOptions && that_present_commonOptions))
        return false;
      if (!this.commonOptions.equals(that.commonOptions))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_blockSizeBytes = true && (isSetBlockSizeBytes());
    list.add(present_blockSizeBytes);
    if (present_blockSizeBytes)
      list.add(blockSizeBytes);

    boolean present_persisted = true && (isSetPersisted());
    list.add(present_persisted);
    if (present_persisted)
      list.add(persisted);

    boolean present_recursive = true && (isSetRecursive());
    list.add(present_recursive);
    if (present_recursive)
      list.add(recursive);

    boolean present_ttlNotUsed = true && (isSetTtlNotUsed());
    list.add(present_ttlNotUsed);
    if (present_ttlNotUsed)
      list.add(ttlNotUsed);

    boolean present_mode = true && (isSetMode());
    list.add(present_mode);
    if (present_mode)
      list.add(mode);

    boolean present_ttlActionNotUsed = true && (isSetTtlActionNotUsed());
    list.add(present_ttlActionNotUsed);
    if (present_ttlActionNotUsed)
      list.add(ttlActionNotUsed.getValue());

    boolean present_commonOptions = true && (isSetCommonOptions());
    list.add(present_commonOptions);
    if (present_commonOptions)
      list.add(commonOptions);

    return list.hashCode();
  }

  @Override
  public int compareTo(CreateFileTOptions other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetBlockSizeBytes()).compareTo(other.isSetBlockSizeBytes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlockSizeBytes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blockSizeBytes, other.blockSizeBytes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPersisted()).compareTo(other.isSetPersisted());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPersisted()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.persisted, other.persisted);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRecursive()).compareTo(other.isSetRecursive());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRecursive()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.recursive, other.recursive);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTtlNotUsed()).compareTo(other.isSetTtlNotUsed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTtlNotUsed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ttlNotUsed, other.ttlNotUsed);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMode()).compareTo(other.isSetMode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMode()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.mode, other.mode);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTtlActionNotUsed()).compareTo(other.isSetTtlActionNotUsed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTtlActionNotUsed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ttlActionNotUsed, other.ttlActionNotUsed);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCommonOptions()).compareTo(other.isSetCommonOptions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCommonOptions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.commonOptions, other.commonOptions);
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
    StringBuilder sb = new StringBuilder("CreateFileTOptions(");
    boolean first = true;

    if (isSetBlockSizeBytes()) {
      sb.append("blockSizeBytes:");
      sb.append(this.blockSizeBytes);
      first = false;
    }
    if (isSetPersisted()) {
      if (!first) sb.append(", ");
      sb.append("persisted:");
      sb.append(this.persisted);
      first = false;
    }
    if (isSetRecursive()) {
      if (!first) sb.append(", ");
      sb.append("recursive:");
      sb.append(this.recursive);
      first = false;
    }
    if (isSetTtlNotUsed()) {
      if (!first) sb.append(", ");
      sb.append("ttlNotUsed:");
      sb.append(this.ttlNotUsed);
      first = false;
    }
    if (isSetMode()) {
      if (!first) sb.append(", ");
      sb.append("mode:");
      sb.append(this.mode);
      first = false;
    }
    if (isSetTtlActionNotUsed()) {
      if (!first) sb.append(", ");
      sb.append("ttlActionNotUsed:");
      if (this.ttlActionNotUsed == null) {
        sb.append("null");
      } else {
        sb.append(this.ttlActionNotUsed);
      }
      first = false;
    }
    if (isSetCommonOptions()) {
      if (!first) sb.append(", ");
      sb.append("commonOptions:");
      if (this.commonOptions == null) {
        sb.append("null");
      } else {
        sb.append(this.commonOptions);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (commonOptions != null) {
      commonOptions.validate();
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

  private static class CreateFileTOptionsStandardSchemeFactory implements SchemeFactory {
    public CreateFileTOptionsStandardScheme getScheme() {
      return new CreateFileTOptionsStandardScheme();
    }
  }

  private static class CreateFileTOptionsStandardScheme extends StandardScheme<CreateFileTOptions> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BLOCK_SIZE_BYTES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.blockSizeBytes = iprot.readI64();
              struct.setBlockSizeBytesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PERSISTED
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.persisted = iprot.readBool();
              struct.setPersistedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // RECURSIVE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.recursive = iprot.readBool();
              struct.setRecursiveIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // TTL_NOT_USED
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.ttlNotUsed = iprot.readI64();
              struct.setTtlNotUsedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // MODE
            if (schemeField.type == org.apache.thrift.protocol.TType.I16) {
              struct.mode = iprot.readI16();
              struct.setModeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // TTL_ACTION_NOT_USED
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.ttlActionNotUsed = alluxio.thrift.TTtlAction.findByValue(iprot.readI32());
              struct.setTtlActionNotUsedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // COMMON_OPTIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.commonOptions = new FileSystemMasterCommonTOptions();
              struct.commonOptions.read(iprot);
              struct.setCommonOptionsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.isSetBlockSizeBytes()) {
        oprot.writeFieldBegin(BLOCK_SIZE_BYTES_FIELD_DESC);
        oprot.writeI64(struct.blockSizeBytes);
        oprot.writeFieldEnd();
      }
      if (struct.isSetPersisted()) {
        oprot.writeFieldBegin(PERSISTED_FIELD_DESC);
        oprot.writeBool(struct.persisted);
        oprot.writeFieldEnd();
      }
      if (struct.isSetRecursive()) {
        oprot.writeFieldBegin(RECURSIVE_FIELD_DESC);
        oprot.writeBool(struct.recursive);
        oprot.writeFieldEnd();
      }
      if (struct.isSetTtlNotUsed()) {
        oprot.writeFieldBegin(TTL_NOT_USED_FIELD_DESC);
        oprot.writeI64(struct.ttlNotUsed);
        oprot.writeFieldEnd();
      }
      if (struct.isSetMode()) {
        oprot.writeFieldBegin(MODE_FIELD_DESC);
        oprot.writeI16(struct.mode);
        oprot.writeFieldEnd();
      }
      if (struct.ttlActionNotUsed != null) {
        if (struct.isSetTtlActionNotUsed()) {
          oprot.writeFieldBegin(TTL_ACTION_NOT_USED_FIELD_DESC);
          oprot.writeI32(struct.ttlActionNotUsed.getValue());
          oprot.writeFieldEnd();
        }
      }
      if (struct.commonOptions != null) {
        if (struct.isSetCommonOptions()) {
          oprot.writeFieldBegin(COMMON_OPTIONS_FIELD_DESC);
          struct.commonOptions.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CreateFileTOptionsTupleSchemeFactory implements SchemeFactory {
    public CreateFileTOptionsTupleScheme getScheme() {
      return new CreateFileTOptionsTupleScheme();
    }
  }

  private static class CreateFileTOptionsTupleScheme extends TupleScheme<CreateFileTOptions> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBlockSizeBytes()) {
        optionals.set(0);
      }
      if (struct.isSetPersisted()) {
        optionals.set(1);
      }
      if (struct.isSetRecursive()) {
        optionals.set(2);
      }
      if (struct.isSetTtlNotUsed()) {
        optionals.set(3);
      }
      if (struct.isSetMode()) {
        optionals.set(4);
      }
      if (struct.isSetTtlActionNotUsed()) {
        optionals.set(5);
      }
      if (struct.isSetCommonOptions()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.isSetBlockSizeBytes()) {
        oprot.writeI64(struct.blockSizeBytes);
      }
      if (struct.isSetPersisted()) {
        oprot.writeBool(struct.persisted);
      }
      if (struct.isSetRecursive()) {
        oprot.writeBool(struct.recursive);
      }
      if (struct.isSetTtlNotUsed()) {
        oprot.writeI64(struct.ttlNotUsed);
      }
      if (struct.isSetMode()) {
        oprot.writeI16(struct.mode);
      }
      if (struct.isSetTtlActionNotUsed()) {
        oprot.writeI32(struct.ttlActionNotUsed.getValue());
      }
      if (struct.isSetCommonOptions()) {
        struct.commonOptions.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.blockSizeBytes = iprot.readI64();
        struct.setBlockSizeBytesIsSet(true);
      }
      if (incoming.get(1)) {
        struct.persisted = iprot.readBool();
        struct.setPersistedIsSet(true);
      }
      if (incoming.get(2)) {
        struct.recursive = iprot.readBool();
        struct.setRecursiveIsSet(true);
      }
      if (incoming.get(3)) {
        struct.ttlNotUsed = iprot.readI64();
        struct.setTtlNotUsedIsSet(true);
      }
      if (incoming.get(4)) {
        struct.mode = iprot.readI16();
        struct.setModeIsSet(true);
      }
      if (incoming.get(5)) {
        struct.ttlActionNotUsed = alluxio.thrift.TTtlAction.findByValue(iprot.readI32());
        struct.setTtlActionNotUsedIsSet(true);
      }
      if (incoming.get(6)) {
        struct.commonOptions = new FileSystemMasterCommonTOptions();
        struct.commonOptions.read(iprot);
        struct.setCommonOptionsIsSet(true);
      }
    }
  }

}

