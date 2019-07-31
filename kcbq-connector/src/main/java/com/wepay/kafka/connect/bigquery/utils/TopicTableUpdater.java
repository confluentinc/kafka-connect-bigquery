package com.wepay.kafka.connect.bigquery.utils;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver.getTopicToTableSingleMatch;

public class TopicTableUpdater {
    public static Map<String, String> updateTopicToDateset (
            Map<String, String> topicToDataset,
            List<Map.Entry<Pattern, String>> patterns,
            List<String> values,
            String valueProperty,
            String patternProperty) {
        for (String value : values) {
            String match = null;
            for (Map.Entry<Pattern, String> pattern : patterns) {
                Matcher patternMatcher = pattern.getKey().matcher(value);
                if (patternMatcher.matches()) {
                    if (match != null) {
                        String secondMatch = pattern.getValue();
                        throw new ConfigException(
                                "Value '" + value
                                        + "' for property '" + valueProperty
                                        + "' matches " + patternProperty
                                        + " regexes for both '" + match
                                        + "' and '" + secondMatch + "'"
                        );
                    }
                    match = pattern.getValue();
                }
            }
            if (match == null) {
                throw new ConfigException(
                        "Value '" + value
                                + "' for property '" + valueProperty
                                + "' failed to match any of the provided " + patternProperty
                                + " regexes"
                );
            }
            topicToDataset.put(value, match);
        }
        return topicToDataset;
    }

    /**
     * Return a Map detailing which BigQuery table each topic should write to.
     *
     * @param config Config that contains properties used to generate the map
     * @return A Map associating Kafka topic names to BigQuery table names.
     */
    public static Map<String, TableId> updateTopicToTable(
            BigQuerySinkConfig config,
            String topicName,
            Map<String, String> topicToDataset,
            Map<String, TableId> topicToTable) {
        Boolean sanitize = config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);
        String match = getTopicToTableSingleMatch(config, topicName);

        if (match == null) {
            match = topicName;
        }
        if (sanitize) {
            match = sanitizeTableName(match);
        }
        Map<String, String> updatedTopicToDateset =
            updateTopicToDateset(
                    topicToDataset,
                    config.getSinglePatterns(config.DATASETS_CONFIG),
                    Arrays.asList(topicName),
                    config.TOPICS_CONFIG,
                    config.DATASETS_CONFIG
            );
        topicToTable.put(topicName, TableId.of(updatedTopicToDateset.get(topicName), match));
        return topicToTable;
    }

    /**
     * Strips illegal characters from a table name. BigQuery only allows alpha-numeric and
     * underscore. Everything illegal is converted to an underscore.
     *
     * @param tableName The table name to sanitize.
     * @return A clean table name with only alpha-numerics and underscores.
     */
    private static String sanitizeTableName(String tableName) {
        return tableName.replaceAll("[^a-zA-Z0-9_]", "_");
    }


}
