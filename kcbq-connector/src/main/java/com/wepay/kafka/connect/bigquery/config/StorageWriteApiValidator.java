package com.wepay.kafka.connect.bigquery.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.*;

public class StorageWriteApiValidator extends MultiPropertyValidator<BigQuerySinkConfig> {

    private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
            UPSERT_ENABLED_CONFIG, DELETE_ENABLED_CONFIG, ENABLE_BATCH_CONFIG
    ));

    protected StorageWriteApiValidator(String propertyName) {
        super(propertyName);
    }

    protected StorageWriteApiValidator() {
        super(USE_STORAGE_WRITE_API_CONFIG);
    }

    @Override
    protected Collection<String> dependents() {
        return DEPENDENTS;
    }

    @Override
    protected Optional<String> doValidate(BigQuerySinkConfig config) {
        if (!config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)) {
            if (config.getBoolean(ENABLE_BATCH_MODE_CONFIG)) {
                return Optional.of("Storage Write Api Batch load is supported only when "
                        + USE_STORAGE_WRITE_API_CONFIG + " is enabled. Either disable batch mode or enable Storage Write" +
                        " API");
            }
            //No legacy modes validation needed if not using new api
            return Optional.empty();
        }
        if (config.getBoolean(UPSERT_ENABLED_CONFIG)) {
            return Optional.of(
                    "Upsert mode is not supported with Storage Write API." +
                            " Either disable Upsert mode or disable Storage Write API");
        } else if (config.getBoolean(DELETE_ENABLED_CONFIG)) {
            return Optional.of(
                    "Delete mode is not supported with Storage Write API." +
                            " Either disable Delete mode or disable Storage Write API");
        } else if (!config.getList(ENABLE_BATCH_CONFIG).isEmpty()) {
            return Optional.of(
                    "Legacy Batch mode is not supported with Storage Write API." +
                            " Either disable Legacy Batch mode or disable Storage Write API");
        }
        return Optional.empty();
    }

    static class StorageWriteApiBatchValidator extends StorageWriteApiValidator {
        StorageWriteApiBatchValidator() {
            super(ENABLE_BATCH_MODE_CONFIG);
        }
    }
}
