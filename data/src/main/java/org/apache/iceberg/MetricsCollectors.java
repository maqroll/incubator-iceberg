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

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

// BORRAR tomada de ValueWriters
// Lo que falte cogerlo de allí.
public class MetricsCollectors {
  private MetricsCollectors() {
  }

  public static MetricsCollector<Void> nulls() {
    /* return NullWriter.INSTANCE;*/
    return null; /* TODO */
  }

  public static MetricsCollector<Boolean> booleans() {
    return null; /* TODO */
  }

  public static MetricsCollector<Integer> ints() {
    return null; /* TODO */
  }

  public static MetricsCollector<Long> longs() {
    return null; /* TODO */
  }

  public static MetricsCollector<Float> floats() {
    return null; /* TODO */
  }

  public static MetricsCollector<Double> doubles() {
    return null; /* TODO */
  }

  public static MetricsCollector<Object> strings() {
    return null; /* TODO */
  }

  public static MetricsCollector<Utf8> utf8s() {
    return null; /* TODO */
  }

  public static MetricsCollector<UUID> uuids() {
    return null; /* TODO */
  }

  public static MetricsCollector<byte[]> fixed(int length) {
    return null; /* TODO */
  }

  public static MetricsCollector<GenericData.Fixed> genericFixed(int length) {
    return null; /* TODO */
  }

  public static MetricsCollector<byte[]> bytes() {
    return null; /* TODO */
  }

  public static MetricsCollector<ByteBuffer> byteBuffers() {
    return null; /* TODO */
  }

  public static MetricsCollector<BigDecimal> decimal(int precision, int scale) {
    return null; /* TODO */
  }
}