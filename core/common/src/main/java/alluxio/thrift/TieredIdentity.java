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
public class TieredIdentity implements org.apache.thrift.TBase<TieredIdentity, TieredIdentity._Fields>, java.io.Serializable, Cloneable, Comparable<TieredIdentity> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TieredIdentity");

  private static final org.apache.thrift.protocol.TField TIERS_FIELD_DESC = new org.apache.thrift.protocol.TField("tiers", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TieredIdentityStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TieredIdentityTupleSchemeFactory());
  }

  private List<LocalityTier> tiers; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TIERS((short)1, "tiers");

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
        case 1: // TIERS
          return TIERS;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TIERS, new org.apache.thrift.meta_data.FieldMetaData("tiers", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, LocalityTier.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TieredIdentity.class, metaDataMap);
  }

  public TieredIdentity() {
  }

  public TieredIdentity(
    List<LocalityTier> tiers)
  {
    this();
    this.tiers = tiers;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TieredIdentity(TieredIdentity other) {
    if (other.isSetTiers()) {
      List<LocalityTier> __this__tiers = new ArrayList<LocalityTier>(other.tiers.size());
      for (LocalityTier other_element : other.tiers) {
        __this__tiers.add(new LocalityTier(other_element));
      }
      this.tiers = __this__tiers;
    }
  }

  public TieredIdentity deepCopy() {
    return new TieredIdentity(this);
  }

  @Override
  public void clear() {
    this.tiers = null;
  }

  public int getTiersSize() {
    return (this.tiers == null) ? 0 : this.tiers.size();
  }

  public java.util.Iterator<LocalityTier> getTiersIterator() {
    return (this.tiers == null) ? null : this.tiers.iterator();
  }

  public void addToTiers(LocalityTier elem) {
    if (this.tiers == null) {
      this.tiers = new ArrayList<LocalityTier>();
    }
    this.tiers.add(elem);
  }

  public List<LocalityTier> getTiers() {
    return this.tiers;
  }

  public TieredIdentity setTiers(List<LocalityTier> tiers) {
    this.tiers = tiers;
    return this;
  }

  public void unsetTiers() {
    this.tiers = null;
  }

  /** Returns true if field tiers is set (has been assigned a value) and false otherwise */
  public boolean isSetTiers() {
    return this.tiers != null;
  }

  public void setTiersIsSet(boolean value) {
    if (!value) {
      this.tiers = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TIERS:
      if (value == null) {
        unsetTiers();
      } else {
        setTiers((List<LocalityTier>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TIERS:
      return getTiers();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TIERS:
      return isSetTiers();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TieredIdentity)
      return this.equals((TieredIdentity)that);
    return false;
  }

  public boolean equals(TieredIdentity that) {
    if (that == null)
      return false;

    boolean this_present_tiers = true && this.isSetTiers();
    boolean that_present_tiers = true && that.isSetTiers();
    if (this_present_tiers || that_present_tiers) {
      if (!(this_present_tiers && that_present_tiers))
        return false;
      if (!this.tiers.equals(that.tiers))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_tiers = true && (isSetTiers());
    list.add(present_tiers);
    if (present_tiers)
      list.add(tiers);

    return list.hashCode();
  }

  @Override
  public int compareTo(TieredIdentity other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTiers()).compareTo(other.isSetTiers());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTiers()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tiers, other.tiers);
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
    StringBuilder sb = new StringBuilder("TieredIdentity(");
    boolean first = true;

    sb.append("tiers:");
    if (this.tiers == null) {
      sb.append("null");
    } else {
      sb.append(this.tiers);
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TieredIdentityStandardSchemeFactory implements SchemeFactory {
    public TieredIdentityStandardScheme getScheme() {
      return new TieredIdentityStandardScheme();
    }
  }

  private static class TieredIdentityStandardScheme extends StandardScheme<TieredIdentity> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TieredIdentity struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TIERS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list26 = iprot.readListBegin();
                struct.tiers = new ArrayList<LocalityTier>(_list26.size);
                LocalityTier _elem27;
                for (int _i28 = 0; _i28 < _list26.size; ++_i28)
                {
                  _elem27 = new LocalityTier();
                  _elem27.read(iprot);
                  struct.tiers.add(_elem27);
                }
                iprot.readListEnd();
              }
              struct.setTiersIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TieredIdentity struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tiers != null) {
        oprot.writeFieldBegin(TIERS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.tiers.size()));
          for (LocalityTier _iter29 : struct.tiers)
          {
            _iter29.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TieredIdentityTupleSchemeFactory implements SchemeFactory {
    public TieredIdentityTupleScheme getScheme() {
      return new TieredIdentityTupleScheme();
    }
  }

  private static class TieredIdentityTupleScheme extends TupleScheme<TieredIdentity> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TieredIdentity struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetTiers()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetTiers()) {
        {
          oprot.writeI32(struct.tiers.size());
          for (LocalityTier _iter30 : struct.tiers)
          {
            _iter30.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TieredIdentity struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list31 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.tiers = new ArrayList<LocalityTier>(_list31.size);
          LocalityTier _elem32;
          for (int _i33 = 0; _i33 < _list31.size; ++_i33)
          {
            _elem32 = new LocalityTier();
            _elem32.read(iprot);
            struct.tiers.add(_elem32);
          }
        }
        struct.setTiersIsSet(true);
      }
    }
  }

}

