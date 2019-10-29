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

package alluxio.job.transform.format.csv;

import alluxio.AlluxioURI;
import alluxio.job.transform.Format;
import alluxio.job.transform.HiveConstants;
import alluxio.job.transform.PartitionInfo;
import alluxio.job.transform.SchemaField;
import alluxio.job.transform.format.ReadWriterUtils;
import alluxio.job.transform.format.TableReader;
import alluxio.job.transform.format.TableRow;
import alluxio.job.transform.format.TableSchema;

import com.google.common.io.Closer;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.csv.AvroCSVReader;
import org.apache.parquet.cli.csv.CSVProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * A reader for reading {@link CsvRow}.
 */
public final class CsvReader implements TableReader {
  private static final Logger LOG = LoggerFactory.getLogger(CsvReader.class);

  private final FileSystem mFs;
  private final AvroCSVReader<Record> mReader;
  private final Closer mCloser;
  private final CsvSchema mSchema;

  /**
   * @param inputPath the input path
   * @param pInfo the partition info
   * @throws IOException when failed to open the input
   */
  private CsvReader(Path inputPath, PartitionInfo pInfo) throws IOException {
    mCloser = Closer.create();
    try {
      Schema schema = buildSchema(Schema.Type.RECORD.getName(), pInfo.getFields());
      mSchema = new CsvSchema(schema);

      Configuration conf = ReadWriterUtils.readNoCacheConf();
      mFs = mCloser.register(inputPath.getFileSystem(conf));
      // TODO(cc): is GZIP_CSV ever returned?
      boolean isGzipped = pInfo.getFormat().equals(Format.GZIP_CSV);
      InputStream input = open(mFs, inputPath, isGzipped);

      CSVProperties props = buildProperties(pInfo.getProperties());

      try {
        mReader = mCloser.register(new AvroCSVReader<>(input, props, schema, Record.class, false));
      } catch (RuntimeException e) {
        throw new IOException("Failed to create CSV reader", e);
      }
    } catch (IOException e) {
      try {
        mCloser.close();
      } catch (IOException ioe) {
        e.addSuppressed(ioe);
      }
      throw e;
    }
  }

  private CSVProperties buildProperties(Map<String, String> properties) {
    CSVProperties.Builder propsBuilder = new CSVProperties.Builder();
    if (properties.containsKey(HiveConstants.LINES_TO_SKIP)) {
      propsBuilder.hasHeader();
      propsBuilder.linesToSkip(Integer.parseInt(properties.get(HiveConstants.LINES_TO_SKIP)));
    }
    if (properties.containsKey(HiveConstants.FIELD_DELIM)) {
      propsBuilder.delimiter(properties.get(HiveConstants.FIELD_DELIM));
    }
    return propsBuilder.build();
  }

  private Schema buildSchema(String name, List<SchemaField> fields) {
    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record(name).fields();
    for (SchemaField field : fields) {
      assembler = buildField(assembler, field);
    }
    return assembler.endRecord();
  }

  private SchemaBuilder.FieldAssembler<Schema> buildField(
      SchemaBuilder.FieldAssembler<Schema> assembler, SchemaField field) {
    String name = field.getName();
    String type = field.getType();
    boolean optional = field.isOptional();
    if (type.equals(HiveConstants.PrimitiveTypes.BOOLEAN)) {
      return optional ? assembler.optionalBoolean(name) : assembler.requiredBoolean(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.TINYINT)
        || type.equals(HiveConstants.PrimitiveTypes.SMALLINT)
        || type.equals(HiveConstants.PrimitiveTypes.INT)) {
      return optional ? assembler.optionalInt(name) : assembler.requiredInt(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.DOUBLE)) {
      return optional ? assembler.optionalDouble(name) : assembler.requiredDouble(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.FLOAT)) {
      return optional ? assembler.optionalFloat(name) : assembler.requiredFloat(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.BIGINT)) {
      return optional ? assembler.optionalLong(name) : assembler.requiredLong(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.STRING)) {
      return optional ? assembler.optionalString(name) : assembler.requiredString(name);
    } else if (type.startsWith(HiveConstants.PrimitiveTypes.DECIMAL)) {
      // TODO(cc): handle optional
      int precision = 10;
      int scale = 0;
      if (type.contains("(")) {
        String param = type.substring(8, type.length() - 1).trim();
        if (type.contains(",")) {
          String[] params = param.split(",");
          precision = Integer.parseInt(params[0].trim());
          scale = Integer.parseInt(params[1].trim());
        } else {
          precision = Integer.parseInt(param);
        }
      }
      return assembler.name(name).type(LogicalTypes.decimal(precision, scale)
          .addToSchema(Schema.create(Schema.Type.BYTES))).noDefault();
    } else if (type.startsWith(HiveConstants.PrimitiveTypes.CHAR)) {
      // TODO(cc)
    } else if (type.startsWith(HiveConstants.PrimitiveTypes.VARCHAR)) {
      // TODO(cc)
    } else if (type.equals(HiveConstants.PrimitiveTypes.BINARY)) {
      // TODO(cc)
    } else if (type.equals(HiveConstants.PrimitiveTypes.DATE)) {
      // TODO(cc)
    } else if (type.equals(HiveConstants.PrimitiveTypes.DATETIME)) {
      // TODO(cc)
    } else if (type.equals(HiveConstants.PrimitiveTypes.TIMESTAMP)) {
      // TODO(cc)
    }
    throw new IllegalArgumentException("Unknown type: " + type + " for field " + name);
  }

  private static InputStream open(FileSystem fs, Path path, boolean isGzipped) throws IOException {
    InputStream stream = fs.open(path);
    if (isGzipped) {
      stream = new GZIPInputStream(stream);
    }
    return stream;
  }

  /**
   * Creates a CSV reader.
   *
   * @param uri the URI to the input
   * @param pInfo the partition info
   * @return the reader
   * @throws IOException when failed to create the reader
   */
  public static CsvReader create(AlluxioURI uri, PartitionInfo pInfo) throws IOException {
    return new CsvReader(new Path(uri.getScheme(), uri.getAuthority().toString(), uri.getPath()),
        pInfo);
  }

  @Override
  public TableSchema getSchema() throws IOException {
    return mSchema;
  }

  @Override
  public TableRow read() throws IOException {
    return mReader.hasNext() ? new CsvRow(mReader.next()) : null;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
