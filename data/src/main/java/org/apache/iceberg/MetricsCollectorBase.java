package org.apache.iceberg;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MetricsCollectorBase<D> implements MetricsCollector<D> {

  private List<MetricsCollector> collectors = null;
  private Integer id;

  public MetricsCollectorBase() {
    // Needed
  }

  public MetricsCollectorBase(List<MetricsCollector> collectors) {
    this.id = null; /* No todos los collectores tienen id */
    this.collectors = collectors;
  }

  public MetricsCollectorBase(int id, List<MetricsCollector> collectors) {
    this.id = id; // Necesitamos el id para devolver las métricas porque estan van indexadas por id de campo pero no utilizamos el id para parsear el Record, va por posición.
    this.collectors = collectors;
  }

/*  @Override
  public void add(D datum) {
    // TODO
    // Itera sobre la lista de colectores
    // y hace un get sobre el id y llama a add con el tipo correcto
  }*/

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
      lowerBounds.putAll(metrics.lowerBounds());
      upperBounds.putAll(metrics.upperBounds());
    }

    return new Metrics(/* TODO */0L, null, valueCounts, nullValueCounts, lowerBounds, upperBounds);
  }
}
