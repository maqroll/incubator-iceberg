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
import org.apache.iceberg.avro.AvroSchemaVisitor;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
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

public class TestMetrics {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()));

  private static final Types.StructType LEAF_STRUCT_TYPE = Types.StructType.of(
      optional(5, "leafLongCol", Types.LongType.get()),
      optional(6, "leafBinaryCol", Types.BinaryType.get())
  );

  private static final Types.StructType NESTED_STRUCT_TYPE = Types.StructType.of(
      required(3, "longCol", Types.LongType.get()),
      required(4, "leafStructCol", LEAF_STRUCT_TYPE)
  );

  private static final Schema NESTED_SCHEMA = new Schema(
      required(1, "intCol", Types.IntegerType.get()),
      required(2, "nestedStructCol", NESTED_STRUCT_TYPE)
  );

  public TestMetrics() {
  }

  @Test
  public void testNestedMetrics() throws IOException {
    // TODO
    MetricsCollector visit = TypeUtil.visit(NESTED_SCHEMA, new BuildMetricsCollector());
    System.out.println(visit);
  }

  @Test
  public void testMetrics() throws IOException {
    // TODO
    MetricsCollector visit = TypeUtil.visit(SCHEMA, new BuildMetricsCollector());
  }

  // Copiado de PartitionKey o TypeToSparkType
  // Los métodos visit en TypeUtil deberían funcionar
  // OJO!! porque no solo necesitamos una estructura para acomodar la métrica
  // sino también como aplicar el registro.
  private static class BuildMetricsCollector
      extends TypeUtil.SchemaVisitor<MetricsCollector> {

    private BuildMetricsCollector(){}

    @Override
    public MetricsCollector schema(
        Schema schema, MetricsCollector metricsCollector) {
      System.out.println(schema);
      System.out.println(metricsCollector);
      return metricsCollector;
      // TODO: throw error
    }

    @Override
    public MetricsCollector struct(Types.StructType struct, List<MetricsCollector> fieldResults) {
      /*return super.struct(struct, fieldResults);*/
      System.out.println("......... struct ...........");
      System.out.println(struct);
      System.out.println(fieldResults);
      System.out.println("............................");
      return new MetricsCollectorTesting<Record>(fieldResults);
    }

    @Override
    public MetricsCollector field(Types.NestedField field, MetricsCollector fieldResult) {
      /*return super.field(field, fieldResult);*/
      System.out.println("......... field ...........");
      System.out.println(field);
      System.out.println(fieldResult);
      System.out.println("............................");
      return new MetricsCollectorTesting(field.fieldId(), Arrays.asList(fieldResult));
    }

    @Override
    public MetricsCollector list(Types.ListType list, MetricsCollector elementResult) {
      /*return super.list(list, elementResult);*/
      System.out.println("......... list ...........");
      System.out.println(list);
      System.out.println(elementResult);
      System.out.println("............................");
      return null;
    }

    @Override
    public MetricsCollector map(Types.MapType map, MetricsCollector keyResult, MetricsCollector valueResult) {
      /*return super.map(map, keyResult, valueResult);*/
      System.out.println("......... map ...........");
      System.out.println(map);
      System.out.println(keyResult);
      System.out.println(valueResult);
      System.out.println("............................");
      return null;
    }

    @Override
    public MetricsCollector primitive(Type.PrimitiveType primitive) {
      System.out.println("......... primitive ...........");
      /*return super.primitive(primitive);*/
      System.out.println(primitive);
      System.out.println("............................");
      return null;
    }
  }

}
