package org.apache.iceberg;

import java.util.List;

public class MetricsCollectorTesting<D> implements MetricsCollector<D> {

  private final List<MetricsCollector> collectors;
  private final Integer id;

  public MetricsCollectorTesting(List<MetricsCollector> collectors) {
    this.id = null; /* No todos los collectores tienen id */
    this.collectors = collectors;
  }

  public MetricsCollectorTesting(int id, List<MetricsCollector> collectors) {
    this.id = id;
    this.collectors = collectors;
  }

  @Override
  public void add(D record) {
    // TODO
    // Itera sobre la lista de colectores
    // y hace un get sobre el id y llama a add con el tipo correcto
  }

  @Override
  public Metrics getMetrics() {
    // TODO
    return null;
  }
}
