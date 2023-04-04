package com.wepay.kafka.connect.bigquery.exception;


import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.rpc.Code;
import java.util.Arrays;

/**
 * Util for storage Write API error responses. This new API uses gRPC protocol.
 * gRPC code : https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.rpc#google.rpc.Code
 */
public class BigQueryStorageWriteApiErrorResponses {

    private static final int INVALID_ARGUMENT_CODE = 3;
    private static final String PERMISSION_DENIED = "PERMISSION_DENIED";
    private static final String NOT_EXIST = "(or it may not exist)";
    private static final String NOT_FOUND = "Not found: table";
    private static final String TABLE_IS_DELETED = "Table is deleted";
    private static final String[] retriableCodes = {Code.INTERNAL.name(), Code.ABORTED.name(), Code.CANCELLED.name()};

    /**
     * Expected BigQuery Table does not exist
     * @param errorMessage Message from the received exception
     * @return Returns true if message contains table missing substrings
     */
    public static boolean isTableMissing(String errorMessage) {
        return (errorMessage.contains(PERMISSION_DENIED) && errorMessage.contains(NOT_EXIST))
                || errorMessage.contains(NOT_FOUND)
                || errorMessage.contains(TABLE_IS_DELETED);
    }

    /**
     * The list of retriable code is taken write api sample codes and gRpc code page
     * @param errorMessage Message from the received exception
     * @return Retruns true if the exception is retriable
     */
    public static boolean isRetriableError(String errorMessage) {
        return Arrays.stream(retriableCodes).anyMatch(errorMessage::contains);
    }

    public static boolean isMalformedRequest(int gRpcErrorCode) {
        return gRpcErrorCode == INVALID_ARGUMENT_CODE;
    }

    /**
     * Indicates user input is incorrect
     * @param exception Exception received on append call
     * @return Returns if the exception is due to bad input
     */
    public static boolean isMalformedRequest(Exception exception) {
        return exception instanceof Exceptions.AppendSerializtionError
                && isMalformedRequest(((Exceptions.AppendSerializtionError) exception)
                .getStatus().getCode().value());
    }
}