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

package alluxio.cli.job.command;

import alluxio.cli.CommandUtils;
import alluxio.cli.fs.command.AbstractFileSystemCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Sorts a parquet file using Z-Ordering.
 */
@ThreadSafe
public final class ZOrderCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ZOrderCommand.class);
  private static final Configuration CONF = new Configuration();

  private static final String BATCH_OPTION_NAME = "batch";
  private static final String COLUMNS_OPTION_NAME = "columns";
  private static final int DEFAULT_BATCH = 10000;

  private static final Option BATCH_OPTION =
      Option.builder()
          .longOpt(BATCH_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .desc("number of rows to sort in-memory in a batch, default value is " + DEFAULT_BATCH)
          .build();

  private static final Option COLUMNS_OPTION =
      Option.builder()
          .longOpt(COLUMNS_OPTION_NAME)
          .required(true)
          .hasArg(true)
          .numberOfArgs(1)
          .desc("columns to be used as sort keys, separated by comma")
          .build();

  /**
   * creates the zorder command.
   *
   * @param fsContext the Alluxio filesystem client
   */
  public ZOrderCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "zorder";
  }

  @Override
  public String getUsage() {
    return "zorder --columns column1,column2 [--batch batch_size] input.parquet output.parquet";
  }

  @Override
  public String getDescription() {
    return "Sorts a parquet file by Z-Ordering the specified columns.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(COLUMNS_OPTION)
        .addOption(BATCH_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    File input = new File(cl.getArgs()[0]);
    File output = new File(cl.getArgs()[1]);
    String[] columns = cl.getOptionValue(COLUMNS_OPTION_NAME).split(",");
    int batch = cl.hasOption(BATCH_OPTION_NAME) ? Integer.parseInt(
        cl.getOptionValue(BATCH_OPTION_NAME)) : DEFAULT_BATCH;

    ParquetMetadata footer = ParquetFileReader.readFooter(CONF, new Path(input.getAbsolutePath()));
    Schema schema = new AvroSchemaConverter().convert(footer.getFileMetaData().getSchema());

    // Since the file might be so large that memory cannot hold all records,
    // we split the records into batches, and do in-memory sorting on one batch at a time.
    // Each sorted batch is persisted to one intermediate file.
    // Then merge the intermediate files to the output file.
    Queue<File> sorted = new LinkedList<>();
    try (ParquetReader<Record> reader = createParquetReader(input)) {
      List<Record> records = new ArrayList<>(batch);
      Record record = null;
      while ((record = reader.read()) != null) {
        // Collect a new batch.
        records.clear();
        records.add(record);
        while (records.size() < batch && (record = reader.read()) != null) {
          records.add(record);
        }
        // Sort the batch.
        records.sort(Comparator.comparing(r -> zorder(r, columns)));
        // Write the sorted batch to an intermediate file.
        File sortedFile = File.createTempFile("alluxio", ".parquet");
        sortedFile.delete();
        writeRecords(sortedFile, schema, records);
        // Add the intermediate file to a queue for merge-sort.
        sorted.add(sortedFile);
      }
    }

    // Merge sort the intermediate files, each file contains sorted records,
    // each time, merge one batch of files to a new intermediate file.
    // Continue the process until there is only one file left.
    List<File> toMerge = new ArrayList<>(batch);
    List<ParquetReader<Record>> readers = new ArrayList<>(batch);
    ParquetWriter<Record> writer = null;
    while (sorted.size() > 1) {
      try {
        while (readers.size() < batch && !sorted.isEmpty()) {
          File f = sorted.poll();
          toMerge.add(f);
          readers.add(createParquetReader(f));
        }
        PriorityQueue<ZOrderRecord> pq = new PriorityQueue<>();
        for (ParquetReader<Record> reader : readers) {
          Record record = reader.read();
          if (record != null) {
            pq.add(new ZOrderRecord(record, zorder(record, columns), reader));
          }
        }
        File mergedFile = File.createTempFile("alluxio", ".parquet");
        mergedFile.delete();
        writer = createParquetWriter(mergedFile, schema);
        while (!pq.isEmpty()) {
          ZOrderRecord top = pq.poll();
          writer.write(top.getRecord());
          Record next = top.getReader().read();
          if (next != null) {
            pq.add(new ZOrderRecord(next, zorder(next, columns), top.getReader()));
          }
        }
        sorted.add(mergedFile);
      } finally {
        if (writer != null) {
          writer.close();
        }
        for (ParquetReader<Record> reader : readers) {
          reader.close();
        }
        readers.clear();
        for (File f : toMerge) {
          f.delete();
        }
        toMerge.clear();
      }
    }

    // The last remaining file is the output.
    Files.move(sorted.poll(), output);

    return 0;
  }

  private static final class ZOrderRecord implements Comparable<ZOrderRecord> {
    private Record mRecord;
    private String mZOrder;
    private ParquetReader<Record> mReader;

    public ZOrderRecord(Record record, String zorder, ParquetReader<Record> reader) {
      mRecord = record;
      mZOrder = zorder;
      mReader = reader;
    }

    public Record getRecord() {
      return mRecord;
    }

    public ParquetReader<Record> getReader() {
      return mReader;
    }

    @Override
    public int compareTo(ZOrderRecord o) {
      return mZOrder.compareTo(o.mZOrder);
    }
  }

  /**
   * Creates a parquet reader.
   *
   * @param input the input file
   * @return the reader
   * @throws IOException when failed to create the reader
   */
  @VisibleForTesting
  public static ParquetReader<Record> createParquetReader(File input) throws IOException {
    return AvroParquetReader.<Record>builder(
        new Path(input.getAbsolutePath()))
        .disableCompatibility()
        .withDataModel(GenericData.get())
        .withConf(CONF)
        .build();
  }

  private static ParquetWriter<Record> createParquetWriter(File output, Schema schema)
      throws IOException {
    return AvroParquetWriter.<Record>builder(new Path(output.getAbsolutePath()))
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
        .withConf(CONF)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
        .withDictionaryPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
        .withDictionaryEncoding(true)
        .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
        .withDataModel(GenericData.get())
        .withSchema(schema)
        .build();
  }

  /**
   * Writes the records to the output file in the parquet format.
   *
   * @param file the output file
   * @param schema the schema
   * @param records the records
   * @throws IOException when I/O exception happens during writing the records
   */
  @VisibleForTesting
  public static void writeRecords(File file, Schema schema, List<Record> records)
      throws IOException {
    try (ParquetWriter<Record> writer = createParquetWriter(file, schema)) {
      for (Record record : records) {
        writer.write(record);
      }
    }
  }

  /**
   * Algorithm to compute zorder:
   * 1. compute 32-bit binary representation of the hash codes of the specified column values
   * 2. interleave those bits to form the zorder, that is, take the 31th bits of all the binaries,
   * then take the 30th bits, etc.
   *
   * Example:
   * record: {a:1, b:2}
   * columns: [a, b]
   * Then the bits are [01, 10],
   * then take "0" from "01", then take "1" from "10",
   * then take "1" from "01", then take "0" from "10",
   * so the final zorder is 0110.
   *
   * @param record the columns of a row
   * @param columns columns to be used to compute the zorder
   * @return the zorder value
   */
  @VisibleForTesting
  public static String zorder(Record record, String[] columns) {
    Map<String, Integer> hashCodes = new HashMap<>(columns.length);
    for (String column : columns) {
      hashCodes.put(column, record.get(column).hashCode());
    }

    List<String> binaryCodes = new ArrayList<>(columns.length);
    for (String column : columns) {
      binaryCodes.add(StringUtils.leftPad(Integer.toBinaryString(hashCodes.get(column)),
          32, '0'));
    }
    StringBuilder z = new StringBuilder();
    for (int i = 0; i < 32; i++) {
      for (String code : binaryCodes) {
        z.append(code.charAt(i));
      }
    }
    return z.toString();
  }
}
