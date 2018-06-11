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
public class TAclEntry implements org.apache.thrift.TBase<TAclEntry, TAclEntry._Fields>, java.io.Serializable, Cloneable, Comparable<TAclEntry> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TAclEntry");

  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField SUBJECT_FIELD_DESC = new org.apache.thrift.protocol.TField("subject", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField ACTIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("actions", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField DEFAULT_ENTRY_FIELD_DESC = new org.apache.thrift.protocol.TField("defaultEntry", org.apache.thrift.protocol.TType.BOOL, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TAclEntryStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TAclEntryTupleSchemeFactory());
  }

  private TAclEntryType type; // optional
  private String subject; // optional
  private List<TAclAction> actions; // optional
  private boolean defaultEntry; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see TAclEntryType
     */
    TYPE((short)1, "type"),
    SUBJECT((short)2, "subject"),
    ACTIONS((short)3, "actions"),
    DEFAULT_ENTRY((short)4, "defaultEntry");

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
        case 1: // TYPE
          return TYPE;
        case 2: // SUBJECT
          return SUBJECT;
        case 3: // ACTIONS
          return ACTIONS;
        case 4: // DEFAULT_ENTRY
          return DEFAULT_ENTRY;
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
  private static final int __DEFAULTENTRY_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.TYPE,_Fields.SUBJECT,_Fields.ACTIONS,_Fields.DEFAULT_ENTRY};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TAclEntryType.class)));
    tmpMap.put(_Fields.SUBJECT, new org.apache.thrift.meta_data.FieldMetaData("subject", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ACTIONS, new org.apache.thrift.meta_data.FieldMetaData("actions", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TAclAction.class))));
    tmpMap.put(_Fields.DEFAULT_ENTRY, new org.apache.thrift.meta_data.FieldMetaData("defaultEntry", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TAclEntry.class, metaDataMap);
  }

  public TAclEntry() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TAclEntry(TAclEntry other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetType()) {
      this.type = other.type;
    }
    if (other.isSetSubject()) {
      this.subject = other.subject;
    }
    if (other.isSetActions()) {
      List<TAclAction> __this__actions = new ArrayList<TAclAction>(other.actions.size());
      for (TAclAction other_element : other.actions) {
        __this__actions.add(other_element);
      }
      this.actions = __this__actions;
    }
    this.defaultEntry = other.defaultEntry;
  }

  public TAclEntry deepCopy() {
    return new TAclEntry(this);
  }

  @Override
  public void clear() {
    this.type = null;
    this.subject = null;
    this.actions = null;
    setDefaultEntryIsSet(false);
    this.defaultEntry = false;
  }

  /**
   * 
   * @see TAclEntryType
   */
  public TAclEntryType getType() {
    return this.type;
  }

  /**
   * 
   * @see TAclEntryType
   */
  public TAclEntry setType(TAclEntryType type) {
    this.type = type;
    return this;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  public String getSubject() {
    return this.subject;
  }

  public TAclEntry setSubject(String subject) {
    this.subject = subject;
    return this;
  }

  public void unsetSubject() {
    this.subject = null;
  }

  /** Returns true if field subject is set (has been assigned a value) and false otherwise */
  public boolean isSetSubject() {
    return this.subject != null;
  }

  public void setSubjectIsSet(boolean value) {
    if (!value) {
      this.subject = null;
    }
  }

  public int getActionsSize() {
    return (this.actions == null) ? 0 : this.actions.size();
  }

  public java.util.Iterator<TAclAction> getActionsIterator() {
    return (this.actions == null) ? null : this.actions.iterator();
  }

  public void addToActions(TAclAction elem) {
    if (this.actions == null) {
      this.actions = new ArrayList<TAclAction>();
    }
    this.actions.add(elem);
  }

  public List<TAclAction> getActions() {
    return this.actions;
  }

  public TAclEntry setActions(List<TAclAction> actions) {
    this.actions = actions;
    return this;
  }

  public void unsetActions() {
    this.actions = null;
  }

  /** Returns true if field actions is set (has been assigned a value) and false otherwise */
  public boolean isSetActions() {
    return this.actions != null;
  }

  public void setActionsIsSet(boolean value) {
    if (!value) {
      this.actions = null;
    }
  }

  public boolean isDefaultEntry() {
    return this.defaultEntry;
  }

  public TAclEntry setDefaultEntry(boolean defaultEntry) {
    this.defaultEntry = defaultEntry;
    setDefaultEntryIsSet(true);
    return this;
  }

  public void unsetDefaultEntry() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __DEFAULTENTRY_ISSET_ID);
  }

  /** Returns true if field defaultEntry is set (has been assigned a value) and false otherwise */
  public boolean isSetDefaultEntry() {
    return EncodingUtils.testBit(__isset_bitfield, __DEFAULTENTRY_ISSET_ID);
  }

  public void setDefaultEntryIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __DEFAULTENTRY_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((TAclEntryType)value);
      }
      break;

    case SUBJECT:
      if (value == null) {
        unsetSubject();
      } else {
        setSubject((String)value);
      }
      break;

    case ACTIONS:
      if (value == null) {
        unsetActions();
      } else {
        setActions((List<TAclAction>)value);
      }
      break;

    case DEFAULT_ENTRY:
      if (value == null) {
        unsetDefaultEntry();
      } else {
        setDefaultEntry((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TYPE:
      return getType();

    case SUBJECT:
      return getSubject();

    case ACTIONS:
      return getActions();

    case DEFAULT_ENTRY:
      return isDefaultEntry();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TYPE:
      return isSetType();
    case SUBJECT:
      return isSetSubject();
    case ACTIONS:
      return isSetActions();
    case DEFAULT_ENTRY:
      return isSetDefaultEntry();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TAclEntry)
      return this.equals((TAclEntry)that);
    return false;
  }

  public boolean equals(TAclEntry that) {
    if (that == null)
      return false;

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_subject = true && this.isSetSubject();
    boolean that_present_subject = true && that.isSetSubject();
    if (this_present_subject || that_present_subject) {
      if (!(this_present_subject && that_present_subject))
        return false;
      if (!this.subject.equals(that.subject))
        return false;
    }

    boolean this_present_actions = true && this.isSetActions();
    boolean that_present_actions = true && that.isSetActions();
    if (this_present_actions || that_present_actions) {
      if (!(this_present_actions && that_present_actions))
        return false;
      if (!this.actions.equals(that.actions))
        return false;
    }

    boolean this_present_defaultEntry = true && this.isSetDefaultEntry();
    boolean that_present_defaultEntry = true && that.isSetDefaultEntry();
    if (this_present_defaultEntry || that_present_defaultEntry) {
      if (!(this_present_defaultEntry && that_present_defaultEntry))
        return false;
      if (this.defaultEntry != that.defaultEntry)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_type = true && (isSetType());
    list.add(present_type);
    if (present_type)
      list.add(type.getValue());

    boolean present_subject = true && (isSetSubject());
    list.add(present_subject);
    if (present_subject)
      list.add(subject);

    boolean present_actions = true && (isSetActions());
    list.add(present_actions);
    if (present_actions)
      list.add(actions);

    boolean present_defaultEntry = true && (isSetDefaultEntry());
    list.add(present_defaultEntry);
    if (present_defaultEntry)
      list.add(defaultEntry);

    return list.hashCode();
  }

  @Override
  public int compareTo(TAclEntry other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetType()).compareTo(other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSubject()).compareTo(other.isSetSubject());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSubject()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.subject, other.subject);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetActions()).compareTo(other.isSetActions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetActions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.actions, other.actions);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDefaultEntry()).compareTo(other.isSetDefaultEntry());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDefaultEntry()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.defaultEntry, other.defaultEntry);
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
    StringBuilder sb = new StringBuilder("TAclEntry(");
    boolean first = true;

    if (isSetType()) {
      sb.append("type:");
      if (this.type == null) {
        sb.append("null");
      } else {
        sb.append(this.type);
      }
      first = false;
    }
    if (isSetSubject()) {
      if (!first) sb.append(", ");
      sb.append("subject:");
      if (this.subject == null) {
        sb.append("null");
      } else {
        sb.append(this.subject);
      }
      first = false;
    }
    if (isSetActions()) {
      if (!first) sb.append(", ");
      sb.append("actions:");
      if (this.actions == null) {
        sb.append("null");
      } else {
        sb.append(this.actions);
      }
      first = false;
    }
    if (isSetDefaultEntry()) {
      if (!first) sb.append(", ");
      sb.append("defaultEntry:");
      sb.append(this.defaultEntry);
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

  private static class TAclEntryStandardSchemeFactory implements SchemeFactory {
    public TAclEntryStandardScheme getScheme() {
      return new TAclEntryStandardScheme();
    }
  }

  private static class TAclEntryStandardScheme extends StandardScheme<TAclEntry> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TAclEntry struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.type = alluxio.thrift.TAclEntryType.findByValue(iprot.readI32());
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SUBJECT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.subject = iprot.readString();
              struct.setSubjectIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ACTIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list16 = iprot.readListBegin();
                struct.actions = new ArrayList<TAclAction>(_list16.size);
                TAclAction _elem17;
                for (int _i18 = 0; _i18 < _list16.size; ++_i18)
                {
                  _elem17 = alluxio.thrift.TAclAction.findByValue(iprot.readI32());
                  struct.actions.add(_elem17);
                }
                iprot.readListEnd();
              }
              struct.setActionsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DEFAULT_ENTRY
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.defaultEntry = iprot.readBool();
              struct.setDefaultEntryIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TAclEntry struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.type != null) {
        if (struct.isSetType()) {
          oprot.writeFieldBegin(TYPE_FIELD_DESC);
          oprot.writeI32(struct.type.getValue());
          oprot.writeFieldEnd();
        }
      }
      if (struct.subject != null) {
        if (struct.isSetSubject()) {
          oprot.writeFieldBegin(SUBJECT_FIELD_DESC);
          oprot.writeString(struct.subject);
          oprot.writeFieldEnd();
        }
      }
      if (struct.actions != null) {
        if (struct.isSetActions()) {
          oprot.writeFieldBegin(ACTIONS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, struct.actions.size()));
            for (TAclAction _iter19 : struct.actions)
            {
              oprot.writeI32(_iter19.getValue());
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetDefaultEntry()) {
        oprot.writeFieldBegin(DEFAULT_ENTRY_FIELD_DESC);
        oprot.writeBool(struct.defaultEntry);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TAclEntryTupleSchemeFactory implements SchemeFactory {
    public TAclEntryTupleScheme getScheme() {
      return new TAclEntryTupleScheme();
    }
  }

  private static class TAclEntryTupleScheme extends TupleScheme<TAclEntry> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TAclEntry struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetType()) {
        optionals.set(0);
      }
      if (struct.isSetSubject()) {
        optionals.set(1);
      }
      if (struct.isSetActions()) {
        optionals.set(2);
      }
      if (struct.isSetDefaultEntry()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetType()) {
        oprot.writeI32(struct.type.getValue());
      }
      if (struct.isSetSubject()) {
        oprot.writeString(struct.subject);
      }
      if (struct.isSetActions()) {
        {
          oprot.writeI32(struct.actions.size());
          for (TAclAction _iter20 : struct.actions)
          {
            oprot.writeI32(_iter20.getValue());
          }
        }
      }
      if (struct.isSetDefaultEntry()) {
        oprot.writeBool(struct.defaultEntry);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TAclEntry struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.type = alluxio.thrift.TAclEntryType.findByValue(iprot.readI32());
        struct.setTypeIsSet(true);
      }
      if (incoming.get(1)) {
        struct.subject = iprot.readString();
        struct.setSubjectIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list21 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, iprot.readI32());
          struct.actions = new ArrayList<TAclAction>(_list21.size);
          TAclAction _elem22;
          for (int _i23 = 0; _i23 < _list21.size; ++_i23)
          {
            _elem22 = alluxio.thrift.TAclAction.findByValue(iprot.readI32());
            struct.actions.add(_elem22);
          }
        }
        struct.setActionsIsSet(true);
      }
      if (incoming.get(3)) {
        struct.defaultEntry = iprot.readBool();
        struct.setDefaultEntryIsSet(true);
      }
    }
  }

}

