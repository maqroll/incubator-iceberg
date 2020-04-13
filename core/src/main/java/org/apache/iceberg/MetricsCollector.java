package org.apache.iceberg;

import java.nio.ByteBuffer;
import java.util.Map;

public interface MetricsCollector<D> {
  // TODO
  // Necesitamos construir los comparadores para los diferentes elementos
  // private Long rowCount = null; -> lo dejamos en el fileAppender
  // private Map<Integer, Long> columnSizes = null; -> devuelve siempre null
  // private Map<Integer, Long> valueCounts = null; -> necesitamos saber si es nulo o no
  // private Map<Integer, Long> nullValueCounts = null; -> necesitamos saber si es nulo o no
  // private Map<Integer, ByteBuffer> lowerBounds = null; -> necesitamos comparadores específicos
  // private Map<Integer, ByteBuffer> upperBounds = null; -> necesitamos comparadores específicos

  public void add(D record);

  // El metricscollector de nivel superior se conectará top-down para recuperar las métricas completas.
  public Metrics getMetrics();
}
