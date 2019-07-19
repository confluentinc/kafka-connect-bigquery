package com.wepay.kafka.connect.bigquery.preprocess;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class SinkRecordFilter {
    private BigQuerySinkConfig bigQueryConfig;
    private RecordConverter recordConverter;

    private String filterType = null;
    private String transformConfigFile = null;
    private ArrayList<String[]> filterConditions;


    public SinkRecordFilter(BigQuerySinkConfig bigQueryConfig) {
        this.bigQueryConfig = bigQueryConfig;
        this.recordConverter = bigQueryConfig.getRecordConverter();

        if (this.bigQueryConfig.getString(this.bigQueryConfig.INCLUDE_CONDITION_CONFIG) != null) {
            this.filterType = this.bigQueryConfig.INCLUDE_KAFKA_DATA_CONFIG;
            this.transformConfigFile = this.bigQueryConfig.getString(bigQueryConfig.INCLUDE_KAFKA_DATA_CONFIG);
        } else if (this.bigQueryConfig.getString(bigQueryConfig.EXCLUDE_CONDITION_CONFIG) != null) {
            this.filterType = this.bigQueryConfig.EXCLUDE_CONDITION_CONFIG;
            this.transformConfigFile = this.bigQueryConfig.getString(bigQueryConfig.EXCLUDE_CONDITION_CONFIG);
        }

        this.filterConditions = parseCondition(transformConfigFile);

    }


    private ArrayList<String[]> parseCondition(String filename) {
        ArrayList<String[]> or_conditions = new ArrayList<>();
        if (filename != null) {
            try (InputStream input = new FileInputStream(filename)) {
                Properties props = new Properties();

                // load a properties file
                props.load(input);

                for (Map.Entry<Object, Object> cond: props.entrySet()){
                    try {
                        if (cond.getValue() != null) {
                            String[] and_conditions = ((String)cond.getValue()).split(",");
                            or_conditions.add(and_conditions);
                        }
                    } catch (ClassCastException castException) {
                        throw new ClassCastException("Condition values needs to be strings. " +
                                "Correct value format: fieldname1=regex1, fieldname2=regex2, ...");
                    }
                }

            } catch (IOException io) {
                io.printStackTrace();
            }
        }
        return or_conditions;
    }

    private boolean satisfyCondition(Map<String, Object> convertedRecord) throws ConfigException {
        // filter messages
        for (String[] and_conditions: this.filterConditions) {
            boolean satisfy = true;
            for (String and_condition: and_conditions) {
                String[] cond_pair = and_condition.split("=");
                if (cond_pair.length != 2) {
                    throw new ConfigException("Invalid expression for conditions: " +
                            "should only have one equality sign"
                    );
                }
                if (cond_pair[0] == null) {
                    throw new ConfigException("Fields should not be Null. " +
                            "fields should have format {}:{}:{}..., with `:` stands for nested");
                }
                String[] nested_fields = cond_pair[0].split(":");
                Map<String, Object> cur_record = convertedRecord;
                for (int i = 0; i < nested_fields.length - 1; i++) {
                    if (nested_fields[i] == null) {
                        throw new ConfigException("Fields should not be Null. " +
                                "fields should have format {}:{}:{}..., with `:` stands for nested");
                    }

                    // proceed only if field is present and its value is nested
                    if (convertedRecord.containsKey(nested_fields[i]) && cur_record.get(nested_fields[i]) instanceof Map) {
                        cur_record = (Map<String, Object>) (cur_record.get(nested_fields[i]));
                    } else {
                        satisfy = false;
                        break;
                    }
                }

                // check if the nested fields match with what's in the config file
                if (!satisfy) {
                    break;
                }

                // if nested fields match, check the last field which should have string value
                if (!((cur_record.containsKey(nested_fields[nested_fields.length-1]) &&
                        cur_record.get(nested_fields[nested_fields.length-1]).getClass() == cond_pair[1].getClass() &&
                        Pattern.matches(cond_pair[1], (String)(cur_record.get(nested_fields[nested_fields.length-1])))))){
                    satisfy = false;
                    break;
                }
            }
            // if any of the [ ] || [ ] || [ ] satisfies, return true
            if (satisfy) {
                return true;
            }
        };
        return false;
    }


    public boolean shouldSkip(SinkRecord record) {
        Map<String, Object> convertRecord = (Map<String, Object>)(this.recordConverter.convertRecord(record));
        switch (this.filterType) {
            case "includeCondition":
                return satisfyCondition(convertRecord) ? false: true;
            case "excludeCondition":
                return satisfyCondition(convertRecord) ? true: false;
            default:
                return false;
        }

    }

    public String getFilterType() {
        return this.filterType;
    }

    public ArrayList<String[]> getFilterConditions() {
        return filterConditions;
    }

}
