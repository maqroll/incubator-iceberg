package org.apache.iceberg.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class BaseLearning {
  private static final Configuration CONF = new Configuration();
  private static final Tables TABLES = new HadoopTables(CONF);
  private static final File TABLE_LOCATION = new File("/tmp/iceberg");

  private final FileFormat format;
  private final Schema SCHEMA;
  private Metrics metrics; // last seen Metrics TODO:delete

  public BaseLearning(Schema schema, FileFormat format) {
    this.SCHEMA = schema;
    this.format = format;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  private String sharedTableLocation = null;
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

  public void dump(Table table) {
    for (HistoryEntry historyEntry : sharedTable.history()) {
      Snapshot snapshot = sharedTable.snapshot(historyEntry.snapshotId());
      System.out.println(historyEntry.snapshotId() + "," + snapshot.manifestListLocation() + "," + snapshot.operation());
      for (ManifestFile manifest : snapshot.manifests()) {
        System.out.println(manifest.path());
      }
      ;
    }
  }

  //@Before
  public Table createTable(final String testPath) throws IOException {
    File testLocation = new File(TABLE_LOCATION,testPath);
    File location = new File(testLocation,format.toString());

    FileUtils.deleteDirectory(location);
    location.mkdirs();
    this.sharedTableLocation = location.toString();

    this.partitionSpec = PartitionSpec.builderFor(SCHEMA).bucket("id",10).build();

    this.sharedTable = TABLES.create(
        SCHEMA,
        partitionSpec,
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()),
        sharedTableLocation);

    return this.sharedTable;
  }

  public DataFile writeFile(String location, String filename, List<Record> records) throws IOException {
    Path path = new Path(location, filename);
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

    GenericRecord partition = null;
/*
    Transform<Long, Integer> bucket = Transforms.<Long>bucket(Types.LongType.get(), 10);
    List<Integer> buckets = records.stream().map((e) -> bucket.apply((Long) e.getField("id"))).collect(Collectors.toList());
    Integer min = buckets.stream().min(Comparator.comparingInt(a -> a)).get();
    Integer max = buckets.stream().max(Comparator.comparingInt(a -> a)).get();
    if (min.equals(max)) {
      partition = GenericRecord.create(partitionSpec.partitionType());
      partition.set(0,min);
      System.out.println(filename + "-> partition:" + min);
    } else {
      System.out.println(filename + "-> partition N/A");
    }*/

    switch (fileFormat) {
      case AVRO:
        FileAppender avroAppender = Avro.write(fromPath(path, CONF))
            .schema(SCHEMA)
            .createWriterFunc(DataWriter::create)
            .named(fileFormat.name())
            .build();

        MetricsAppender metricsAppender = new MetricsAppender(avroAppender,SCHEMA);

        try {
          metricsAppender.addAll(records);
        } finally {
          metricsAppender.close();
        }

        DataFiles.Builder builder = DataFiles.builder(partitionSpec);
        if (partition != null) { builder.withPartition(partition); }

        metrics = metricsAppender.metrics();

        return builder
            .withInputFile(HadoopInputFile.fromPath(path, CONF))
            .withMetrics(metricsAppender.metrics())
            .build();

      case PARQUET:
        FileAppender<Record> orcAppender = Parquet.write(fromPath(path, CONF))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build();
        try {
          orcAppender.addAll(records);
        } finally {
          orcAppender.close();
        }

        DataFiles.Builder builder1 = DataFiles.builder(partitionSpec);

        if (partition != null) { builder1.withPartition(partition);}

        metrics = orcAppender.metrics(); // TODO delete

        return builder1
            .withInputFile(HadoopInputFile.fromPath(path, CONF))
            .withMetrics(orcAppender.metrics())
            .build();

      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }
  }
}
