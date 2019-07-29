package com.wepay.kafka.connect.bigquery.preprocess;

import com.jayway.jsonpath.*;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SinkRecordFilter {
    private BigQuerySinkConfig bigQueryConfig;
    private RecordConverter recordConverter;

    private String configType = null;
    private String configString = null;
    private JsonPath jsonPathCondition = null;
    private static final Logger logger = LoggerFactory.getLogger(SinkRecordFilter.class);

    public SinkRecordFilter(BigQuerySinkConfig bigQueryConfig) {
        this.bigQueryConfig = bigQueryConfig;
        this.recordConverter = bigQueryConfig.getRecordConverter();

        if (this.bigQueryConfig.getString(this.bigQueryConfig.INCLUDE_CONDITION_CONFIG) != null) {
            this.configType = this.bigQueryConfig.INCLUDE_CONDITION_CONFIG;
            this.configString = this.bigQueryConfig.getString(this.bigQueryConfig.INCLUDE_CONDITION_CONFIG);

        } else if (this.bigQueryConfig.getString(bigQueryConfig.EXCLUDE_CONDITION_CONFIG) != null) {
            this.configType = this.bigQueryConfig.EXCLUDE_CONDITION_CONFIG;
            this.configString = this.bigQueryConfig.getString(bigQueryConfig.EXCLUDE_CONDITION_CONFIG);
            this.jsonPathCondition = JsonPath.compile(this.configString);
        }

        if (configType != null) {
            try {
                this.jsonPathCondition = JsonPath.compile(this.configString);
            } catch (InvalidPathException e) {
                throw new InvalidPathException("Json Path " + this.configString + "specified in " +
                        configType + "config is incorrectly formatted. " +
                        "Please refer to com.jayway.jsonpath java doc for correct use of jsonpath.");
            }
        }

    }


    public List getFilterFields(Map<String, Object> convertedRecord) {
        List filterList = new ArrayList();

        if (this.configType != null && convertedRecord != null) {
            try {
                // return a list map nodes that satify the specified condition
                filterList = this.jsonPathCondition.read(
                        convertedRecord,
                        Configuration.defaultConfiguration().addOptions(
                                Option.ALWAYS_RETURN_LIST
                        )
                );
            } catch (PathNotFoundException e) {
                logger.debug("Json Path `" + this.configString + "` specified in " + this.configType +
                        " config can not be found in json object. " +
                        "The condition will be marked as NOT SATISFIED by default.");
            }

        }

        return filterList;
    }

    public boolean satisfyCondition(Map<String, Object> convertedRecord) throws ConfigException {
        return getFilterFields(convertedRecord).size() == 0 ? false: true;
    }


    public boolean shouldSkip(SinkRecord record) {
        Map<String, Object> convertRecord = (Map<String, Object>)(this.recordConverter.convertRecord(record));
        switch (this.configType) {
            case "includeCondition":
                return satisfyCondition(convertRecord) ? false: true;
            case "excludeCondition":
                return satisfyCondition(convertRecord) ? true: false;
            default:
                return false;
        }

    }

    public String getConfigType() {
        return this.configType;
    }

    public String getConfigString() {
        return this.configString;
    }

    public JsonPath getJsonPathCondition() {
        return this.jsonPathCondition;
    }

}
