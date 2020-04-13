package org.apache.iceberg.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static com.google.common.collect.Iterables.*;
import static org.apache.iceberg.DataFiles.fromInputFile;
import static org.apache.iceberg.expressions.Expressions.*;
import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestLearning {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()));

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" }
    };
  }

  private final FileFormat format;

  public TestLearning(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  private Table sharedTable = null;
  private List<Record> file1Records = null;
  private List<Record> file2Records = null;
  private List<Record> file3Records = null;
  private List<Record> file1FirstSnapshotRecords = null;
  private List<Record> file2FirstSnapshotRecords = null;
  private List<Record> file3FirstSnapshotRecords = null;
  private List<Record> file1SecondSnapshotRecords = null;
  private List<Record> file2SecondSnapshotRecords = null;
  private List<Record> file3SecondSnapshotRecords = null;

  private PartitionSpec partitionSpec;

  @Test
  public void testAppend() throws IOException {
    BaseLearning base = new BaseLearning(SCHEMA,format);

    sharedTable = base.createTable("TEST_APPEND");

    Record record = GenericRecord.create(SCHEMA);

    this.file1Records = new ArrayList<Record>();

    file1Records.add(record.copy(ImmutableMap.of("id", 1L, "data", UUID.randomUUID().toString())));
    DataFile file1 = base.writeFile(sharedTable.location(), format.addExtension("file-1"), file1Records);

    this.file2Records = new ArrayList<Record>();
    file2Records.add(record.copy(ImmutableMap.of("id", 1L, "data", UUID.randomUUID().toString())));
    DataFile file2 = base.writeFile(sharedTable.location(), format.addExtension("file-2"), file2Records);

    this.file3Records = new ArrayList<Record>();
    file3Records.add(record.copy(ImmutableMap.of("id", 1L, "data", UUID.randomUUID().toString())));
    DataFile file3 = base.writeFile(sharedTable.location(), format.addExtension("file-3"), file3Records);

    sharedTable.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .commit();

    sharedTable.newAppend()
        .appendFile(file3)
        .commit();
  }

  @Test
  public void testOverwrite() throws IOException {
    BaseLearning base = new BaseLearning(SCHEMA,format);

    sharedTable = base.createTable("TEST_OVERWRITE");

    Record record = GenericRecord.create(SCHEMA);

    this.file1Records = new ArrayList<Record>();
    file1Records.add(record.copy(ImmutableMap.of("id", 60L, "data", UUID.randomUUID().toString())));
    DataFile file1 = base.writeFile(sharedTable.location(), format.addExtension("file-1"), file1Records);

    this.file2Records = new ArrayList<Record>();
    file2Records.add(record.copy(ImmutableMap.of("id", 1L, "data", UUID.randomUUID().toString())));
    DataFile file2 = base.writeFile(sharedTable.location(), format.addExtension("file-2"), file2Records);

    this.file3Records = new ArrayList<Record>();
    file3Records.add(record.copy(ImmutableMap.of("id", 1L, "data", UUID.randomUUID().toString())));
    DataFile file3 = base.writeFile(sharedTable.location(), format.addExtension("file-3"), file3Records);

    sharedTable.newAppend()
        .appendFile(file1)
        .commit();

    sharedTable.newAppend()
        .appendFile(file2)
        .commit();

    // Overwrite works on whole files
    // Cannot delete file where some, but not all, rows match filter ref(name="id") == 1: file:/tmp/iceberg/TEST_OVERWRITE/AVRO/file-1.avro
    sharedTable.newOverwrite()
        //.overwriteByRowFilter(Expressions.alwaysTrue())
        .overwriteByRowFilter(equal("id",1L))
        .addFile(file3)
        .commit();

    base.dump(sharedTable);
  }
}
