package org.apache.iceberg.data;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsCollector;
import org.apache.iceberg.MetricsCollectorBase;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public void testSimpleMetrics() {
    Schema SIMPLE_SCHEMA = new Schema(
        optional(1, "intCol", Types.IntegerType.get()),
        optional(2, "int2Col", Types.IntegerType.get())
    );

    MetricsCollector metricsCollector = TypeUtil.visit(SIMPLE_SCHEMA, new BuildMetricsCollector());

    List<Record> records = new ArrayList<>();
    Record record = GenericRecord.create(SIMPLE_SCHEMA);

    records.add(record.copy(ImmutableMap.of("intCol", 4)));
    records.add(record.copy(ImmutableMap.of("intCol", 4)));
    records.add(record.copy());


    for (Record r : records) {
      metricsCollector.add(r);
    }

    Metrics metrics = metricsCollector.getMetrics();

    System.out.println("record count:" + metrics.recordCount());
    System.out.println("value counts:" + metrics.valueCounts());
    System.out.println("null value counts:" + metrics.nullValueCounts());
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

  private static class MetricsCollectorRecord extends MetricsCollectorBase<Record> {
    List<MetricsCollector> collectors;
    Long count = 0L;

    MetricsCollectorRecord(List<MetricsCollector> collectors) {
      this.collectors = collectors;
    }

    @Override
    public void add(Record record) {
      count++;
      for (int i = 0; i < collectors.size(); i++) {
        collectors.get(i).add(record.get(i));
      }
    }

    @Override
    public Metrics getMetrics() {
      Map<Integer, Long> valueCounts = new HashMap<>();
      Map<Integer, Long> nullValueCounts = new HashMap<>();
      Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
      Map<Integer, ByteBuffer> upperBounds = new HashMap<>();

      for (MetricsCollector metricsCollector : collectors) {
        Metrics metrics = metricsCollector.getMetrics();

        valueCounts.putAll(metrics.valueCounts());
        nullValueCounts.putAll(metrics.nullValueCounts());
        if (metrics.lowerBounds() != null) lowerBounds.putAll(metrics.lowerBounds());
        if (metrics.upperBounds() != null) upperBounds.putAll(metrics.upperBounds());
      }

      return new Metrics(count,null, valueCounts, nullValueCounts, lowerBounds, upperBounds);
    }
  }

  private static class MetricsCollectorInt extends MetricsCollectorBase<Integer> {
    Integer max, min; // Valor por defecto
    Long values = 0L;
    Long nulls = 0L;
    Integer id;

    MetricsCollectorInt(Integer id) {
      this.id = id;
    }

    @Override
    public void add(Integer datum) {
      // IDEA Todos los tipos primitivos deberían heredar de uno que tuviese el mayor y el menor!!
      values++;
      if (datum == null) {
        nulls++;
      } else {
        if (max == null || datum > max) {
          max = datum;
        }
        if (min == null || datum < min) {
          min = datum;
        }
      }
    }

    @Override
    public Metrics getMetrics() {
      return new Metrics(values, null, ImmutableMap.of(id, values), ImmutableMap.of(id, nulls), min == null? null : ImmutableMap.of(id, ByteBuffer.allocate(4).putInt(min)), max==null? null : ImmutableMap.of(id, ByteBuffer.allocate(4).putInt(max)));
    }
  }

  // Copiado de PartitionKey o TypeToSparkType
  // Los métodos visit en TypeUtil deberían funcionar
  // OJO!! porque no solo necesitamos una estructura para acomodar la métrica
  private static class BuildMetricsCollector
      // sino también como aplicar el registro.
  extends TypeUtil.SchemaVisitor<MetricsCollector> {

    private BuildMetricsCollector() {
    }

    @Override
    public MetricsCollector schema(
        Schema schema, MetricsCollector metricsCollector) {
      return metricsCollector;
      // TODO: throw error
    }

    @Override
    public MetricsCollector struct(Types.StructType struct, List<MetricsCollector> fieldResults) {
      return new MetricsCollectorRecord(fieldResults);
    }

    @Override
    public MetricsCollector field(Types.NestedField field, MetricsCollector fieldResult) {
      // OJO porque el tipo primitivo se resuelve en primitive y se recibe como fieldResult
      return new MetricsCollectorInt(field.fieldId());
    }

    @Override
    public MetricsCollector list(Types.ListType list, MetricsCollector elementResult) {
      return null;
    }

    @Override
    public MetricsCollector map(Types.MapType map, MetricsCollector keyResult, MetricsCollector valueResult) {
      return null;
    }

    @Override
    public MetricsCollector primitive(Type.PrimitiveType primitive) {
      return null;
    }
  }

}
