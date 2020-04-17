package org.apache.iceberg.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.directory.api.util.Hex;
import org.apache.iceberg.*;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestLearning {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()));


  private static final Types.StructType NESTED_SIMPLE_SCHEMA = Types.StructType.of(
      optional(7, "nested_int", Types.IntegerType.get())
  );

  private static final Types.ListType LIST_TYPE = Types.ListType.ofRequired(8, Types.StructType.of(
      optional(70, "nested_int70", Types.IntegerType.get())
  ));

  private static final Types.MapType MAP_TYPE = Types.MapType.ofRequired(9,10,Types.LongType.get(),Types.LongType.get());

  private static final Types.StructType LEAF_STRUCT_TYPE = Types.StructType.of(
      optional(5, "leafLongCol", Types.LongType.get()),
      optional(6, "leafBinaryCol", Types.BinaryType.get())
  );

  private static final Types.StructType NESTED_STRUCT_TYPE = Types.StructType.of(
      required(3, "longCol", Types.LongType.get()),
      optional(4, "leafStructCol", LEAF_STRUCT_TYPE)
  );

  private static final Schema NESTED_SCHEMA = new Schema(
      required(100, "id", Types.IntegerType.get()),
      required(200, "nestedStructCol", NESTED_STRUCT_TYPE),
      optional(700, "list", LIST_TYPE),
      optional(1100,"map", MAP_TYPE)
  );

  private static final Schema SIMPLE_SCHEMA = new Schema(
      optional( 100, "id", Types.IntegerType.get()),
      optional(400, "long", Types.LongType.get()),
      optional(200, "nested", NESTED_SIMPLE_SCHEMA),
      optional(500, "list", LIST_TYPE),
      optional(600, "map", MAP_TYPE)
  );


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
  public void testMetricsSimple() throws IOException {
    BaseLearning base = new BaseLearning(SIMPLE_SCHEMA, format);

    sharedTable = base.createTable("TEST_METRICS_SIMPLE");

    Record record = GenericRecord.create(SIMPLE_SCHEMA);
    Record nested = GenericRecord.create(NESTED_SIMPLE_SCHEMA);

    this.file1Records = new ArrayList<>();

    file1Records.add(record.copy(ImmutableMap.of("id",1)));
    file1Records.add(record.copy(ImmutableMap.of("id",1)));
    file1Records.add(record.copy(ImmutableMap.of("id",1, "long", 4L)));
    file1Records.add(record.copy(ImmutableMap.of("id",1, "nested", nested.copy(ImmutableMap.of()))));

    DataFile file1 = base.writeFile(sharedTable.location(),format.addExtension("file-1"), file1Records);

    Metrics m = base.getMetrics();

    System.out.println("record count: " + m.recordCount());
    System.out.println("value counts: " + m.valueCounts());
    System.out.println("null values count:" + m.nullValueCounts());
  }


  @Test
  public void testMetrics() throws IOException {
    // I'm going to use this method to extract metrics for parquet.
    BaseLearning base = new BaseLearning(NESTED_SCHEMA, format);

    sharedTable = base.createTable("TEST_METRICS");

    Record record = GenericRecord.create(NESTED_SCHEMA);
    Record record2 = GenericRecord.create(NESTED_STRUCT_TYPE);
    Record record3 = GenericRecord.create(LEAF_STRUCT_TYPE);

    this.file1Records = new ArrayList<>();

    file1Records.add(record.copy(ImmutableMap.of("id", 1, "nestedStructCol", record2.copy(ImmutableMap.of("longCol", 1L, "leafStructCol",record3.copy(ImmutableMap.of("leafLongCol", 2L, "leafBinaryCol", ByteBuffer.wrap(new byte[] {0, 1, 2}))))))));
    file1Records.add(record.copy(ImmutableMap.of("id", 2, "nestedStructCol", record2.copy(ImmutableMap.of("longCol", 1L, "leafStructCol",record3.copy(ImmutableMap.of("leafLongCol", 2L, "leafBinaryCol", ByteBuffer.wrap(new byte[] {3, 1, 2}))))))));
    file1Records.add(record.copy(ImmutableMap.of("id", 3, "nestedStructCol", record2.copy(ImmutableMap.of("longCol", 1L, "leafStructCol",record3.copy())))));
    file1Records.add(record.copy(ImmutableMap.of("id", 4, "nestedStructCol", record2.copy(ImmutableMap.of("longCol", 1L)))));
    file1Records.add(record.copy(ImmutableMap.of("id", 4, "list",  ImmutableList.of(56L, 57L, 58L),"nestedStructCol", record2.copy(ImmutableMap.of("longCol", 1L)))));
    DataFile file1 = base.writeFile(sharedTable.location(), format.addExtension("file-1"), file1Records);

    Metrics m = base.getMetrics();

    Long recordCount = m.recordCount();
    System.out.println("Record count:" + recordCount);

    //Map<Integer, Long> integerLongMap = m.columnSizes(); DISCARDED
    //Map<Integer, ByteBuffer> integerByteBufferMap = m.lowerBounds();
    //Map<Integer, ByteBuffer> integerByteBufferMap1 = m.upperBounds();

    Map<Integer, ByteBuffer> lowerBounds = m.lowerBounds();
    System.out.println("Lower bounds");
    System.out.println("1 -> " + lowerBounds.get(1).getInt());
    System.out.println("3 -> " + lowerBounds.get(3).getLong());
    System.out.println("6 -> " + new String(Hex.encodeHex(lowerBounds.get(6).array())));
    for(Integer i : lowerBounds.keySet()) {
      System.out.println(i);
    }


    Map<Integer, ByteBuffer> upperBounds = m.upperBounds();
    System.out.println("Upper bounds");
    System.out.println("1 -> " + upperBounds.get(1).getInt());
    System.out.println("3 -> " + upperBounds.get(3).getLong());
    System.out.println("6 -> " + new String(Hex.encodeHex(upperBounds.get(6).array())));
    for(Integer i : upperBounds.keySet()) {
      System.out.println(i);
    }


    Map<Integer, Long> nullValueCounts = m.nullValueCounts();
    Map<Integer, Long> valueCounts = m.valueCounts();

    // solo se genera sobre los campos sencillos
    System.out.println("Null value counts");
    Set<Integer> integers = nullValueCounts.keySet();
    for (Integer i : integers) {
      System.out.println(i + "->" + nullValueCounts.get(i));
    }

    System.out.println("value counts");
    integers = valueCounts.keySet();
    for (Integer i : integers) {
      System.out.println(i + "->" + valueCounts.get(i));
    }

  }


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
