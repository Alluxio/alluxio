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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-9-5")
public class WorkerInfo implements org.apache.thrift.TBase<WorkerInfo, WorkerInfo._Fields>, java.io.Serializable, Cloneable, Comparable<WorkerInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("WorkerInfo");

  private static final org.apache.thrift.protocol.TField ID_FIELD_DESC = new org.apache.thrift.protocol.TField("id", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField ADDRESS_FIELD_DESC = new org.apache.thrift.protocol.TField("address", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField LAST_CONTACT_SEC_FIELD_DESC = new org.apache.thrift.protocol.TField("lastContactSec", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField STATE_FIELD_DESC = new org.apache.thrift.protocol.TField("state", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField CAPACITY_BYTES_FIELD_DESC = new org.apache.thrift.protocol.TField("capacityBytes", org.apache.thrift.protocol.TType.I64, (short)5);
  private static final org.apache.thrift.protocol.TField USED_BYTES_FIELD_DESC = new org.apache.thrift.protocol.TField("usedBytes", org.apache.thrift.protocol.TType.I64, (short)6);
  private static final org.apache.thrift.protocol.TField STARTTIME_MS_FIELD_DESC = new org.apache.thrift.protocol.TField("starttimeMs", org.apache.thrift.protocol.TType.I64, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new WorkerInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new WorkerInfoTupleSchemeFactory());
  }

  public long id; // required
  public NetAddress address; // required
  public int lastContactSec; // required
  public String state; // required
  public long capacityBytes; // required
  public long usedBytes; // required
  public long starttimeMs; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ID((short)1, "id"),
    ADDRESS((short)2, "address"),
    LAST_CONTACT_SEC((short)3, "lastContactSec"),
    STATE((short)4, "state"),
    CAPACITY_BYTES((short)5, "capacityBytes"),
    USED_BYTES((short)6, "usedBytes"),
    STARTTIME_MS((short)7, "starttimeMs");

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
        case 1: // ID
          return ID;
        case 2: // ADDRESS
          return ADDRESS;
        case 3: // LAST_CONTACT_SEC
          return LAST_CONTACT_SEC;
        case 4: // STATE
          return STATE;
        case 5: // CAPACITY_BYTES
          return CAPACITY_BYTES;
        case 6: // USED_BYTES
          return USED_BYTES;
        case 7: // STARTTIME_MS
          return STARTTIME_MS;
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
  private static final int __ID_ISSET_ID = 0;
  private static final int __LASTCONTACTSEC_ISSET_ID = 1;
  private static final int __CAPACITYBYTES_ISSET_ID = 2;
  private static final int __USEDBYTES_ISSET_ID = 3;
  private static final int __STARTTIMEMS_ISSET_ID = 4;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ID, new org.apache.thrift.meta_data.FieldMetaData("id", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.ADDRESS, new org.apache.thrift.meta_data.FieldMetaData("address", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NetAddress.class)));
    tmpMap.put(_Fields.LAST_CONTACT_SEC, new org.apache.thrift.meta_data.FieldMetaData("lastContactSec", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STATE, new org.apache.thrift.meta_data.FieldMetaData("state", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CAPACITY_BYTES, new org.apache.thrift.meta_data.FieldMetaData("capacityBytes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.USED_BYTES, new org.apache.thrift.meta_data.FieldMetaData("usedBytes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.STARTTIME_MS, new org.apache.thrift.meta_data.FieldMetaData("starttimeMs", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WorkerInfo.class, metaDataMap);
  }

  public WorkerInfo() {
  }

  public WorkerInfo(
    long id,
    NetAddress address,
    int lastContactSec,
    String state,
    long capacityBytes,
    long usedBytes,
    long starttimeMs)
  {
    this();
    this.id = id;
    setIdIsSet(true);
    this.address = address;
    this.lastContactSec = lastContactSec;
    setLastContactSecIsSet(true);
    this.state = state;
    this.capacityBytes = capacityBytes;
    setCapacityBytesIsSet(true);
    this.usedBytes = usedBytes;
    setUsedBytesIsSet(true);
    this.starttimeMs = starttimeMs;
    setStarttimeMsIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public WorkerInfo(WorkerInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.id = other.id;
    if (other.isSetAddress()) {
      this.address = new NetAddress(other.address);
    }
    this.lastContactSec = other.lastContactSec;
    if (other.isSetState()) {
      this.state = other.state;
    }
    this.capacityBytes = other.capacityBytes;
    this.usedBytes = other.usedBytes;
    this.starttimeMs = other.starttimeMs;
  }

  public WorkerInfo deepCopy() {
    return new WorkerInfo(this);
  }

  @Override
  public void clear() {
    setIdIsSet(false);
    this.id = 0;
    this.address = null;
    setLastContactSecIsSet(false);
    this.lastContactSec = 0;
    this.state = null;
    setCapacityBytesIsSet(false);
    this.capacityBytes = 0;
    setUsedBytesIsSet(false);
    this.usedBytes = 0;
    setStarttimeMsIsSet(false);
    this.starttimeMs = 0;
  }

  public long getId() {
    return this.id;
  }

  public WorkerInfo setId(long id) {
    this.id = id;
    setIdIsSet(true);
    return this;
  }

  public void unsetId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ID_ISSET_ID);
  }

  /** Returns true if field id is set (has been assigned a value) and false otherwise */
  public boolean isSetId() {
    return EncodingUtils.testBit(__isset_bitfield, __ID_ISSET_ID);
  }

  public void setIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ID_ISSET_ID, value);
  }

  public NetAddress getAddress() {
    return this.address;
  }

  public WorkerInfo setAddress(NetAddress address) {
    this.address = address;
    return this;
  }

  public void unsetAddress() {
    this.address = null;
  }

  /** Returns true if field address is set (has been assigned a value) and false otherwise */
  public boolean isSetAddress() {
    return this.address != null;
  }

  public void setAddressIsSet(boolean value) {
    if (!value) {
      this.address = null;
    }
  }

  public int getLastContactSec() {
    return this.lastContactSec;
  }

  public WorkerInfo setLastContactSec(int lastContactSec) {
    this.lastContactSec = lastContactSec;
    setLastContactSecIsSet(true);
    return this;
  }

  public void unsetLastContactSec() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __LASTCONTACTSEC_ISSET_ID);
  }

  /** Returns true if field lastContactSec is set (has been assigned a value) and false otherwise */
  public boolean isSetLastContactSec() {
    return EncodingUtils.testBit(__isset_bitfield, __LASTCONTACTSEC_ISSET_ID);
  }

  public void setLastContactSecIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __LASTCONTACTSEC_ISSET_ID, value);
  }

  public String getState() {
    return this.state;
  }

  public WorkerInfo setState(String state) {
    this.state = state;
    return this;
  }

  public void unsetState() {
    this.state = null;
  }

  /** Returns true if field state is set (has been assigned a value) and false otherwise */
  public boolean isSetState() {
    return this.state != null;
  }

  public void setStateIsSet(boolean value) {
    if (!value) {
      this.state = null;
    }
  }

  public long getCapacityBytes() {
    return this.capacityBytes;
  }

  public WorkerInfo setCapacityBytes(long capacityBytes) {
    this.capacityBytes = capacityBytes;
    setCapacityBytesIsSet(true);
    return this;
  }

  public void unsetCapacityBytes() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __CAPACITYBYTES_ISSET_ID);
  }

  /** Returns true if field capacityBytes is set (has been assigned a value) and false otherwise */
  public boolean isSetCapacityBytes() {
    return EncodingUtils.testBit(__isset_bitfield, __CAPACITYBYTES_ISSET_ID);
  }

  public void setCapacityBytesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __CAPACITYBYTES_ISSET_ID, value);
  }

  public long getUsedBytes() {
    return this.usedBytes;
  }

  public WorkerInfo setUsedBytes(long usedBytes) {
    this.usedBytes = usedBytes;
    setUsedBytesIsSet(true);
    return this;
  }

  public void unsetUsedBytes() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __USEDBYTES_ISSET_ID);
  }

  /** Returns true if field usedBytes is set (has been assigned a value) and false otherwise */
  public boolean isSetUsedBytes() {
    return EncodingUtils.testBit(__isset_bitfield, __USEDBYTES_ISSET_ID);
  }

  public void setUsedBytesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __USEDBYTES_ISSET_ID, value);
  }

  public long getStarttimeMs() {
    return this.starttimeMs;
  }

  public WorkerInfo setStarttimeMs(long starttimeMs) {
    this.starttimeMs = starttimeMs;
    setStarttimeMsIsSet(true);
    return this;
  }

  public void unsetStarttimeMs() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STARTTIMEMS_ISSET_ID);
  }

  /** Returns true if field starttimeMs is set (has been assigned a value) and false otherwise */
  public boolean isSetStarttimeMs() {
    return EncodingUtils.testBit(__isset_bitfield, __STARTTIMEMS_ISSET_ID);
  }

  public void setStarttimeMsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STARTTIMEMS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ID:
      if (value == null) {
        unsetId();
      } else {
        setId((Long)value);
      }
      break;

    case ADDRESS:
      if (value == null) {
        unsetAddress();
      } else {
        setAddress((NetAddress)value);
      }
      break;

    case LAST_CONTACT_SEC:
      if (value == null) {
        unsetLastContactSec();
      } else {
        setLastContactSec((Integer)value);
      }
      break;

    case STATE:
      if (value == null) {
        unsetState();
      } else {
        setState((String)value);
      }
      break;

    case CAPACITY_BYTES:
      if (value == null) {
        unsetCapacityBytes();
      } else {
        setCapacityBytes((Long)value);
      }
      break;

    case USED_BYTES:
      if (value == null) {
        unsetUsedBytes();
      } else {
        setUsedBytes((Long)value);
      }
      break;

    case STARTTIME_MS:
      if (value == null) {
        unsetStarttimeMs();
      } else {
        setStarttimeMs((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ID:
      return Long.valueOf(getId());

    case ADDRESS:
      return getAddress();

    case LAST_CONTACT_SEC:
      return Integer.valueOf(getLastContactSec());

    case STATE:
      return getState();

    case CAPACITY_BYTES:
      return Long.valueOf(getCapacityBytes());

    case USED_BYTES:
      return Long.valueOf(getUsedBytes());

    case STARTTIME_MS:
      return Long.valueOf(getStarttimeMs());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ID:
      return isSetId();
    case ADDRESS:
      return isSetAddress();
    case LAST_CONTACT_SEC:
      return isSetLastContactSec();
    case STATE:
      return isSetState();
    case CAPACITY_BYTES:
      return isSetCapacityBytes();
    case USED_BYTES:
      return isSetUsedBytes();
    case STARTTIME_MS:
      return isSetStarttimeMs();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof WorkerInfo)
      return this.equals((WorkerInfo)that);
    return false;
  }

  public boolean equals(WorkerInfo that) {
    if (that == null)
      return false;

    boolean this_present_id = true;
    boolean that_present_id = true;
    if (this_present_id || that_present_id) {
      if (!(this_present_id && that_present_id))
        return false;
      if (this.id != that.id)
        return false;
    }

    boolean this_present_address = true && this.isSetAddress();
    boolean that_present_address = true && that.isSetAddress();
    if (this_present_address || that_present_address) {
      if (!(this_present_address && that_present_address))
        return false;
      if (!this.address.equals(that.address))
        return false;
    }

    boolean this_present_lastContactSec = true;
    boolean that_present_lastContactSec = true;
    if (this_present_lastContactSec || that_present_lastContactSec) {
      if (!(this_present_lastContactSec && that_present_lastContactSec))
        return false;
      if (this.lastContactSec != that.lastContactSec)
        return false;
    }

    boolean this_present_state = true && this.isSetState();
    boolean that_present_state = true && that.isSetState();
    if (this_present_state || that_present_state) {
      if (!(this_present_state && that_present_state))
        return false;
      if (!this.state.equals(that.state))
        return false;
    }

    boolean this_present_capacityBytes = true;
    boolean that_present_capacityBytes = true;
    if (this_present_capacityBytes || that_present_capacityBytes) {
      if (!(this_present_capacityBytes && that_present_capacityBytes))
        return false;
      if (this.capacityBytes != that.capacityBytes)
        return false;
    }

    boolean this_present_usedBytes = true;
    boolean that_present_usedBytes = true;
    if (this_present_usedBytes || that_present_usedBytes) {
      if (!(this_present_usedBytes && that_present_usedBytes))
        return false;
      if (this.usedBytes != that.usedBytes)
        return false;
    }

    boolean this_present_starttimeMs = true;
    boolean that_present_starttimeMs = true;
    if (this_present_starttimeMs || that_present_starttimeMs) {
      if (!(this_present_starttimeMs && that_present_starttimeMs))
        return false;
      if (this.starttimeMs != that.starttimeMs)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_id = true;
    list.add(present_id);
    if (present_id)
      list.add(id);

    boolean present_address = true && (isSetAddress());
    list.add(present_address);
    if (present_address)
      list.add(address);

    boolean present_lastContactSec = true;
    list.add(present_lastContactSec);
    if (present_lastContactSec)
      list.add(lastContactSec);

    boolean present_state = true && (isSetState());
    list.add(present_state);
    if (present_state)
      list.add(state);

    boolean present_capacityBytes = true;
    list.add(present_capacityBytes);
    if (present_capacityBytes)
      list.add(capacityBytes);

    boolean present_usedBytes = true;
    list.add(present_usedBytes);
    if (present_usedBytes)
      list.add(usedBytes);

    boolean present_starttimeMs = true;
    list.add(present_starttimeMs);
    if (present_starttimeMs)
      list.add(starttimeMs);

    return list.hashCode();
  }

  @Override
  public int compareTo(WorkerInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetId()).compareTo(other.isSetId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.id, other.id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAddress()).compareTo(other.isSetAddress());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAddress()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.address, other.address);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLastContactSec()).compareTo(other.isSetLastContactSec());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLastContactSec()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lastContactSec, other.lastContactSec);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetState()).compareTo(other.isSetState());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetState()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.state, other.state);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCapacityBytes()).compareTo(other.isSetCapacityBytes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCapacityBytes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.capacityBytes, other.capacityBytes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUsedBytes()).compareTo(other.isSetUsedBytes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUsedBytes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.usedBytes, other.usedBytes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStarttimeMs()).compareTo(other.isSetStarttimeMs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStarttimeMs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.starttimeMs, other.starttimeMs);
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
    StringBuilder sb = new StringBuilder("WorkerInfo(");
    boolean first = true;

    sb.append("id:");
    sb.append(this.id);
    first = false;
    if (!first) sb.append(", ");
    sb.append("address:");
    if (this.address == null) {
      sb.append("null");
    } else {
      sb.append(this.address);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("lastContactSec:");
    sb.append(this.lastContactSec);
    first = false;
    if (!first) sb.append(", ");
    sb.append("state:");
    if (this.state == null) {
      sb.append("null");
    } else {
      sb.append(this.state);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("capacityBytes:");
    sb.append(this.capacityBytes);
    first = false;
    if (!first) sb.append(", ");
    sb.append("usedBytes:");
    sb.append(this.usedBytes);
    first = false;
    if (!first) sb.append(", ");
    sb.append("starttimeMs:");
    sb.append(this.starttimeMs);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (address != null) {
      address.validate();
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

  private static class WorkerInfoStandardSchemeFactory implements SchemeFactory {
    public WorkerInfoStandardScheme getScheme() {
      return new WorkerInfoStandardScheme();
    }
  }

  private static class WorkerInfoStandardScheme extends StandardScheme<WorkerInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, WorkerInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.id = iprot.readI64();
              struct.setIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ADDRESS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.address = new NetAddress();
              struct.address.read(iprot);
              struct.setAddressIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LAST_CONTACT_SEC
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.lastContactSec = iprot.readI32();
              struct.setLastContactSecIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // STATE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.state = iprot.readString();
              struct.setStateIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // CAPACITY_BYTES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.capacityBytes = iprot.readI64();
              struct.setCapacityBytesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // USED_BYTES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.usedBytes = iprot.readI64();
              struct.setUsedBytesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // STARTTIME_MS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.starttimeMs = iprot.readI64();
              struct.setStarttimeMsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, WorkerInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeI64(struct.id);
      oprot.writeFieldEnd();
      if (struct.address != null) {
        oprot.writeFieldBegin(ADDRESS_FIELD_DESC);
        struct.address.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(LAST_CONTACT_SEC_FIELD_DESC);
      oprot.writeI32(struct.lastContactSec);
      oprot.writeFieldEnd();
      if (struct.state != null) {
        oprot.writeFieldBegin(STATE_FIELD_DESC);
        oprot.writeString(struct.state);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(CAPACITY_BYTES_FIELD_DESC);
      oprot.writeI64(struct.capacityBytes);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(USED_BYTES_FIELD_DESC);
      oprot.writeI64(struct.usedBytes);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STARTTIME_MS_FIELD_DESC);
      oprot.writeI64(struct.starttimeMs);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class WorkerInfoTupleSchemeFactory implements SchemeFactory {
    public WorkerInfoTupleScheme getScheme() {
      return new WorkerInfoTupleScheme();
    }
  }

  private static class WorkerInfoTupleScheme extends TupleScheme<WorkerInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, WorkerInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetId()) {
        optionals.set(0);
      }
      if (struct.isSetAddress()) {
        optionals.set(1);
      }
      if (struct.isSetLastContactSec()) {
        optionals.set(2);
      }
      if (struct.isSetState()) {
        optionals.set(3);
      }
      if (struct.isSetCapacityBytes()) {
        optionals.set(4);
      }
      if (struct.isSetUsedBytes()) {
        optionals.set(5);
      }
      if (struct.isSetStarttimeMs()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.isSetId()) {
        oprot.writeI64(struct.id);
      }
      if (struct.isSetAddress()) {
        struct.address.write(oprot);
      }
      if (struct.isSetLastContactSec()) {
        oprot.writeI32(struct.lastContactSec);
      }
      if (struct.isSetState()) {
        oprot.writeString(struct.state);
      }
      if (struct.isSetCapacityBytes()) {
        oprot.writeI64(struct.capacityBytes);
      }
      if (struct.isSetUsedBytes()) {
        oprot.writeI64(struct.usedBytes);
      }
      if (struct.isSetStarttimeMs()) {
        oprot.writeI64(struct.starttimeMs);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, WorkerInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.id = iprot.readI64();
        struct.setIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.address = new NetAddress();
        struct.address.read(iprot);
        struct.setAddressIsSet(true);
      }
      if (incoming.get(2)) {
        struct.lastContactSec = iprot.readI32();
        struct.setLastContactSecIsSet(true);
      }
      if (incoming.get(3)) {
        struct.state = iprot.readString();
        struct.setStateIsSet(true);
      }
      if (incoming.get(4)) {
        struct.capacityBytes = iprot.readI64();
        struct.setCapacityBytesIsSet(true);
      }
      if (incoming.get(5)) {
        struct.usedBytes = iprot.readI64();
        struct.setUsedBytesIsSet(true);
      }
      if (incoming.get(6)) {
        struct.starttimeMs = iprot.readI64();
        struct.setStarttimeMsIsSet(true);
      }
    }
  }

}

