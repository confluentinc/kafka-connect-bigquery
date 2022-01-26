/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class SanitizeFieldNameIT extends BaseConnectorIT {
  private static final Logger logger = LoggerFactory.getLogger(UpsertDeleteBigQuerySinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "kcbq-sink-connector";
  private static final long NUM_RECORDS_PRODUCED = 20;
  private static final int TASKS_MAX = 3;
  private static final String KAFKA_KEY_FIELD_NAME = "kafkaKey";

  private BigQuery bigQuery;

  @Before
  public void setup() {
    bigQuery = newBigQuery();
    startConnect();
  }

  @After
  public void close() {
    bigQuery = null;
    stopConnect();
  }

  @Test
  public void testSanitizeFieldName() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("test-sanitize-field-name");
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

    // setup props for the sink connector
    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    // use the JSON converter with schemas enabled
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    props.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, KAFKA_KEY_FIELD_NAME);

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      String kafkaKey = key(keyConverter, topic, i);
      String kafkaValue = value(valueConverter, topic, i, false);
      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", kafkaKey, kafkaValue, topic);
      connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    List<List<Object>> allRows = readAllRows(bigQuery, table, KAFKA_KEY_FIELD_NAME + ".key_1");
    List<List<Object>> expectedRows = LongStream.range(0, NUM_RECORDS_PRODUCED)
        .mapToObj(i -> Arrays.asList(
            "a string",
            i % 3 == 0,
            i / 0.69,
            Collections.singletonList(i)))
        .collect(Collectors.toList());
    assertEquals(expectedRows, allRows);
  }

  private Converter converter(boolean isKey) {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, isKey);
    return result;
  }

  private String key(Converter converter, String topic, long iteration) {
    final Schema schema = SchemaBuilder.struct()
        .field("key 1", Schema.INT64_SCHEMA)
        .build();

    final Struct struct = new Struct(schema)
        .put("key 1", iteration);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  private String value(Converter converter, String topic, long iteration, boolean tombstone) {
    final Schema schema = SchemaBuilder.struct()
        .optional()
        .field("string field", Schema.STRING_SCHEMA)
        .field("boolean field", Schema.BOOLEAN_SCHEMA)
        .field("float field", Schema.FLOAT64_SCHEMA)
        .build();

    if (tombstone) {
      return new String(converter.fromConnectData(topic, schema, null));
    }

    final Struct struct = new Struct(schema)
        .put("string field", "a string")
        .put("boolean field", iteration % 3 == 0)
        .put("float field", iteration / 0.69);

    return new String(converter.fromConnectData(topic, schema, struct));
  }
}
