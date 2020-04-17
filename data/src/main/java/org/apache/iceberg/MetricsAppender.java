/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsAppender<D> implements FileAppender<D> {

  private final FileAppender<D> appender;
  private final Schema schema;
  private Metrics metrics = null;
  private MetricsConfig metricsConfig = MetricsConfig.getDefault();
  private MetricsCollector metricsCollector;

  public MetricsAppender(FileAppender<D> appender, Schema schema) {
    this.appender = appender;
    this.schema = schema;
    this.metricsCollector = TypeUtil.visit(schema, new BuildMetricsCollector());
  }

  @Override
  public void add(D datum) {
    appender.add(datum);
    metricsCollector.add(datum);
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(metrics != null, "Cannot produce metrics until closed");
    return metrics;
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public void close() throws IOException {
    appender.close();
    metrics = metricsCollector.getMetrics();
  }

  private static class MetricsCollectorMap<K,V> extends MetricsCollectorBase<Map<K,V>> {
    private final MetricsCollector collectorKey;
    private final MetricsCollector collectorValue;

    MetricsCollectorMap(MetricsCollector<K> collectorKey, MetricsCollector<V> collectorValue) {
      this.collectorKey = collectorKey;
      this.collectorValue = collectorValue;
    }

    @Override
    public void add(Map<K,V> map) {
      if (map != null) {
        for(K k: map.keySet()) {
          collectorKey.add(k);
          collectorValue.add(map.get(k));
        }
      } else {
        collectorKey.add(null);
        collectorValue.add(null);
      }
    }

    @Override
    public Metrics getMetrics() {
      // debería procesar para ignorar los bounds?
      Map<Integer, Long> valueCounts = new HashMap<>();
      Map<Integer, Long> nullValueCounts = new HashMap<>();
      Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
      Map<Integer, ByteBuffer> upperBounds = new HashMap<>();

      Metrics metricsKey = collectorKey.getMetrics();
      Metrics metricsValue = collectorValue.getMetrics();

      if (metricsKey.valueCounts() != null) valueCounts.putAll(metricsKey.valueCounts());
      if (metricsValue.valueCounts() != null) valueCounts.putAll(metricsValue.valueCounts());

      if (metricsKey.nullValueCounts() != null) nullValueCounts.putAll(metricsKey.nullValueCounts());
      if (metricsValue.nullValueCounts() != null) nullValueCounts.putAll(metricsValue.nullValueCounts());

      if (metricsKey.lowerBounds() != null) lowerBounds.putAll(metricsKey.lowerBounds());
      if (metricsValue.lowerBounds() != null) lowerBounds.putAll(metricsValue.lowerBounds());

      if (metricsKey.upperBounds() != null) upperBounds.putAll(metricsKey.upperBounds());
      if (metricsValue.upperBounds() != null) upperBounds.putAll(metricsValue.upperBounds());

      return new Metrics(0L,null,valueCounts,nullValueCounts,lowerBounds,upperBounds);
    }
  }

  private static class MetricsCollectorList<L> extends MetricsCollectorBase<List<L>> {

    private final MetricsCollector collector;

    MetricsCollectorList(MetricsCollector<L> collector) {
      this.collector = collector;
    }

    @Override
    public void add(List<L> list) {
      if (list != null) {
        for(L e: list) {
          collector.add(e);
        }
      } else {
        collector.add(null);
      }
    }

    @Override
    public Metrics getMetrics() {
      // debería procesar para ignorar los bounds?
      return collector.getMetrics();
    }
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
        collectors.get(i).add(record == null ? null : record.get(i));
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

      return new Metrics(count, null, valueCounts, nullValueCounts, lowerBounds, upperBounds);
    }
  }

  private abstract static class MetricsCollectorPrimitive<D> implements MetricsCollector<D> {
    private final Comparator<D> comparator;
    D max;
    D min;
    Long values = 0L;
    Long nulls = 0L;
    final Integer id;

    public MetricsCollectorPrimitive(Integer id, Comparator<D> comparator) {
      this.id = id;
      this.comparator = comparator;
    }

    @Override
    public void add(D datum) {
      // IDEA Todos los tipos primitivos deberían heredar de uno que tuviese el mayor y el menor!!
      values++;
      if (datum == null) {
        nulls++;
      } else {
        if (max == null || comparator.compare(datum,max)>0 ) {
          max = datum;
        }
        if (min == null || comparator.compare(datum,min)<0) {
          min = datum;
        }
      }
    }

    abstract ByteBuffer encode(D datum);

    @Override
    public Metrics getMetrics() {
      return new Metrics(values, null, ImmutableMap.of(id, values), ImmutableMap.of(id, nulls), min == null? null : ImmutableMap.of(id, encode(min)), max==null? null : ImmutableMap.of(id, encode(max)));
    }
  }

  private static class MetricsCollectorInt extends MetricsCollectorPrimitive<Integer> {
    public MetricsCollectorInt(Integer id, Comparator<Integer> comparator) { super(id,comparator); }

    @Override
    ByteBuffer encode(Integer datum) {
      return ByteBuffer.allocate(4).putInt(datum);
    }
  }

  private static class MetricsCollectorLong extends MetricsCollectorPrimitive<Long> {
    public MetricsCollectorLong(Integer id, Comparator<Long> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Long datum) {
      return ByteBuffer.allocate(8).putLong(datum);
    }
  }

  // Copiado de PartitionKey o TypeToSparkType
  // Los métodos visit en TypeUtil deberían funcionar
  // OJO!! porque no solo necesitamos una estructura para acomodar la métrica
  // sino también como aplicar el registro.
  private static class BuildMetricsCollector
      extends TypeUtil.SchemaVisitor<MetricsCollector> {

    Types.NestedField currentField = null;

    private BuildMetricsCollector() {
    }

    @Override
    public void beforeField(Types.NestedField field) {
      currentField = field;
    }

    @Override
    public void afterField(Types.NestedField field) {
      currentField = null;
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
      return fieldResult;
    }

    @Override
    public MetricsCollector list(Types.ListType list, MetricsCollector elementResult) {
      return new MetricsCollectorList(elementResult);
    }

    @Override
    public MetricsCollector map(Types.MapType map, MetricsCollector keyResult, MetricsCollector valueResult) {
      return new MetricsCollectorMap(keyResult,valueResult);
    }

    @Override
    public MetricsCollector primitive(Type.PrimitiveType primitive) {
      switch (primitive.typeId()) {
        case BOOLEAN:
//          return BooleanType$.MODULE$;
        case INTEGER:
          return new MetricsCollectorInt(currentField.fieldId(), Comparators.forType(primitive));
        case LONG:
          return new MetricsCollectorLong(currentField.fieldId(), Comparators.forType(primitive));
        case FLOAT:
//          return FloatType$.MODULE$;
        case DOUBLE:
//          return DoubleType$.MODULE$;
        case DATE:
//          return DateType$.MODULE$;
        case TIME:
          // TODO
        case TIMESTAMP:
          // TODO
        case STRING:
//          return StringType$.MODULE$;
        case UUID:
          // use String
//          return StringType$.MODULE$;
        case FIXED:
//          return BinaryType$.MODULE$;
        case BINARY:
//          return BinaryType$.MODULE$;
        case DECIMAL:
//          Types.DecimalType decimal = (Types.DecimalType) primitive;
//          return DecimalType$.MODULE$.apply(decimal.precision(), decimal.scale());
        default:
          return null; /* sobra */
      }
    }
  }
}
