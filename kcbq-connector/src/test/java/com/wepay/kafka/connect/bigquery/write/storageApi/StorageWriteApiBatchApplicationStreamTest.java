package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;

public class StorageWriteApiBatchApplicationStreamTest {
    StorageWriteApiBatchApplicationStream mockedStream = mock(StorageWriteApiBatchApplicationStream.class,
            CALLS_REAL_METHODS);
    TableName mockedTable1 = TableName.of("p", "d", "t1");
    TableName mockedTable2 = TableName.of("p", "d", "t2");
    ApplicationStream mockedApplicationStream1 = mock(ApplicationStream.class);
    ApplicationStream mockedApplicationStream2 = mock(ApplicationStream.class);
    String mockedStreamName1 = "dummyApplicationStream1";
    String mockedStreamName2 = "dummyApplicationStream2";
    Map<TopicPartition, OffsetAndMetadata> mockedOffsets = new HashMap<>();
    List<Object[]> mockedRows = new ArrayList<>();
    SinkRecord mockedSinkRecord = new SinkRecord(
            "t1", 0, null, null, Schema.BOOLEAN_SCHEMA, true, 100);
    ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
    JsonStreamWriter mockedJsonWriter = mock(JsonStreamWriter.class);
    AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
            .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();
    String exceeded30AttemptException = "Exceeded 30 attempts to write to table "
            + mockedTable1.toString() + " ";
    String exceeded0AttemptException = "Exceeded 0 attempts to write to table "
            + mockedTable1.toString() + " ";
    String baseErrorMessage = String.format("Failed to write rows on table %s due to ", mockedTable1.toString());
    ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
    ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);
    Map<Integer, String> errorMapping = new HashMap<>();
    Exceptions.AppendSerializtionError badRecordsException = new Exceptions.AppendSerializtionError(
            3,
            "INVALID_ARGUMENT",
            "DEFAULT",
            errorMapping);
    String malformedExceptionMessage = "Insertion failed at table t1 for following rows:" +
            " \n [row index 0] (Failure reason : f0 field is unknown) ";

    @Before
    public void setup() throws InterruptedException, Descriptors.DescriptorValidationException, IOException {
        mockedStream.tableLocks = new ConcurrentHashMap<>();
        mockedStream.streams = new ConcurrentHashMap<>();
        mockedStream.currentStreams = new ConcurrentHashMap<>();

        errorMapping.put(0, "f0 field is unknown");
        mockedOffsets.put(new TopicPartition("t2", 0), new OffsetAndMetadata(100));
        mockedRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        doNothing().when(mockedApplicationStream1).closeStream();
        doNothing().when(mockedApplicationStream2).closeStream();
        doNothing().when(mockedApplicationStream1).markInactive();
        doNothing().when(mockedApplicationStream2).markInactive();
        doNothing().when(mockedApplicationStream1).finalise();
        doNothing().when(mockedApplicationStream1).commit();
        doNothing().when(mockedStream).attemptSchemaUpdate(any(), any());
        doNothing().when(mockedStream).attemptTableCreation(any(), any());
        doNothing().when(mockedStream).waitRandomTime(anyInt());

        when(mockedJsonWriter.append(any())).thenReturn(mockedResponse);
        when(mockedStream.getAutoCreateTables()).thenReturn(true);
        when(mockedApplicationStream1.canBeMovedToNonActive()).thenReturn(true);
        when(mockedApplicationStream1.isInactive()).thenReturn(true);
        when(mockedApplicationStream2.isInactive()).thenReturn(false);
        when(mockedApplicationStream1.isReadyForOffsetCommit()).thenReturn(false);
        when(mockedApplicationStream2.isReadyForOffsetCommit()).thenReturn(true);
        when(mockedApplicationStream2.getOffsetInformation()).thenReturn(mockedOffsets);
        when(mockedApplicationStream1.writer()).thenReturn(mockedJsonWriter);
        when(mockedStream.getErrantRecordHandler()).thenReturn(mockedErrantRecordHandler);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(mockedErrantReporter);
    }

    private void initialiseStreams() {
        mockedStream.currentStreams.put(mockedTable1.toString(), mockedStreamName1);
        mockedStream.streams.put(mockedTable1.toString(), new LinkedHashMap<>());
        mockedStream.streams.put(mockedTable2.toString(), new LinkedHashMap<>());
        mockedStream.streams.get(mockedTable1.toString()).put(mockedStreamName1, mockedApplicationStream1);
        mockedStream.streams.get(mockedTable2.toString()).put(mockedStreamName2, mockedApplicationStream2);
    }

    private void verifyException(String expectedException) {
        try {
            mockedStream.appendRows(mockedTable1, mockedRows, mockedStreamName1);
        } catch (Exception e) {
            assertEquals(expectedException, e.getMessage());
            assertTrue(e instanceof BigQueryStorageWriteApiConnectException);
        }
    }

    @Test
    public void testShutdown() {
        initialiseStreams();
        mockedStream.shutdown();
        verify(mockedApplicationStream1, times(1)).closeStream();
        verify(mockedApplicationStream2, times(1)).closeStream();
    }

    @Test
    public void testGetCommitableOffsets() {
        initialiseStreams();
        Map<TopicPartition, OffsetAndMetadata> expected = mockedStream.getCommitableOffsets();
        assertEquals(expected, mockedOffsets);
        mockedStream.streams.keySet().forEach(k -> {
            assertEquals(0, mockedStream.streams.get(k).size());
        });

    }

    @Test
    public void testGetNoCommitableOffsets() {
        initialiseStreams();
        when(mockedApplicationStream1.isInactive()).thenReturn(false);
        when(mockedApplicationStream2.isReadyForOffsetCommit()).thenReturn(false);

        Map<TopicPartition, OffsetAndMetadata> expected = mockedStream.getCommitableOffsets();

        assertEquals(expected, Collections.emptyMap());
        mockedStream.streams.keySet().forEach(k -> {
            assertEquals(1, mockedStream.streams.get(k).size());
        });

    }

    @Test
    public void testMayBeCreateStreamSuccess() {
        initialiseStreams();
        ApplicationStream mockedApplicationStream = mock(ApplicationStream.class);

        doReturn(mockedApplicationStream).when(mockedStream).createApplicationStream(mockedTable1.toString(), null);
        when(mockedApplicationStream.getStreamName()).thenReturn("mockedApplicationStream");

        boolean expected = mockedStream.mayBeCreateStream(mockedTable1.toString(), null);

        assertTrue(expected);
        assertEquals("mockedApplicationStream", mockedStream.currentStreams.get(mockedTable1.toString()));
        assertTrue(mockedStream.streams.get(mockedTable1.toString()).containsKey("mockedApplicationStream"));
        assertTrue(mockedStream.streams.get(mockedTable1.toString()).containsValue(mockedApplicationStream));
        verify(mockedApplicationStream1, times(1)).areAllExpectedCallsCompleted();
    }

    @Test
    public void testMayBeCreateStreamFalseForScheduler() {
        initialiseStreams();

        doReturn(null).when(mockedStream).createApplicationStream(mockedTable1.toString(), null);

        boolean expected = mockedStream.mayBeCreateStream(mockedTable1.toString(), null);

        assertFalse(expected);
        assertEquals(mockedStreamName1, mockedStream.currentStreams.get(mockedTable1.toString()));
        assertFalse(mockedStream.streams.get(mockedTable1.toString()).containsKey("mockedApplicationStream"));
    }

    @Test
    public void testMayBeCreateStreamFirstStream() {
        ApplicationStream mockedApplicationStream = mock(ApplicationStream.class);

        doReturn(mockedApplicationStream).when(mockedStream).createApplicationStream(mockedTable1.toString(), null);
        when(mockedApplicationStream.getStreamName()).thenReturn("mockedApplicationStream");

        boolean expected = mockedStream.mayBeCreateStream(mockedTable1.toString(), null);

        assertTrue(expected);
        assertEquals("mockedApplicationStream", mockedStream.currentStreams.get(mockedTable1.toString()));
        assertEquals(1, mockedStream.streams.get(mockedTable1.toString()).size());
        assertEquals(mockedApplicationStream, mockedStream.streams.get(mockedTable1.toString()).get("mockedApplicationStream"));
    }

    @Test
    public void testUpdateOffsetsOnStream() {
        initialiseStreams();

        String streamName = mockedStream.updateOffsetsOnStream(mockedTable1.toString(), mockedRows);
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);

        assertEquals(mockedStreamName1, streamName);
        verify(mockedApplicationStream1, times(1)).updateOffsetInformation(captor.capture());

        Map<TopicPartition, OffsetAndMetadata> actualOffset = captor.getValue();
        assertEquals(1, actualOffset.size());

        actualOffset.forEach((key, value) -> {
            assertEquals("t1", key.topic());
            assertEquals(0, key.partition());
            assertEquals(101, value.offset());
        });
    }

    @Test
    public void testAppendSuccess() throws Exception {
        initialiseStreams();

        mockedStream.currentStreams.put(mockedTable1.toString(), "newStream");
        when(mockedApplicationStream1.areAllExpectedCallsCompleted()).thenReturn(true);
        when(mockedResponse.get()).thenReturn(successResponse);

        mockedStream.appendRows(mockedTable1, mockedRows, mockedStreamName1);

        verify(mockedApplicationStream1, times(1)).increaseAppendCallCount();
        verify(mockedApplicationStream1, times(1)).increaseCompletedCallsCount();
        verify(mockedApplicationStream1, times(1)).areAllExpectedCallsCompleted();
        verify(mockedApplicationStream1, times(1)).finalise();
        verify(mockedApplicationStream1, times(1)).commit();
    }

    @Test
    public void testAppendSchemaUpdateEventualSuccess() throws Exception {
        initialiseStreams();
        AppendRowsResponse badResponse = AppendRowsResponse.newBuilder()
                .setUpdatedSchema(TableSchema.newBuilder().build())
                .build();
        ExecutionException schemaException = new ExecutionException(
                new Throwable("Destination table schema mismatch due to SCHEMA_MISMATCH_EXTRA_FIELDS"));

        mockedStream.currentStreams.put(mockedTable1.toString(), "newStream");
        when(mockedApplicationStream1.areAllExpectedCallsCompleted()).thenReturn(true);
        when(mockedResponse.get()).thenReturn(badResponse).thenThrow(schemaException).thenReturn(successResponse);

        mockedStream.appendRows(mockedTable1, mockedRows, mockedStreamName1);

        verify(mockedStream, times(1)).attemptSchemaUpdate(any(), any());
        verify(mockedApplicationStream1, times(1)).increaseCompletedCallsCount();
        verify(mockedApplicationStream1, times(1)).areAllExpectedCallsCompleted();
        verify(mockedApplicationStream1, times(1)).finalise();
        verify(mockedApplicationStream1, times(1)).commit();
    }

    @Test
    public void testAppendSchemaUpdateEventualFail() throws Exception {
        initialiseStreams();
        AppendRowsResponse badResponse = AppendRowsResponse.newBuilder()
                .setUpdatedSchema(TableSchema.newBuilder().build())
                .build();
        ExecutionException schemaException = new ExecutionException(
                new Throwable("Destination table schema mismatch due to SCHEMA_MISMATCH_EXTRA_FIELDS"));

        when(mockedResponse.get()).thenReturn(badResponse).thenThrow(schemaException);

        verifyException(exceeded30AttemptException);

        verify(mockedStream, times(1)).attemptSchemaUpdate(any(), any());
        verify(mockedApplicationStream1, times(0)).increaseCompletedCallsCount();
    }

    @Test
    public void testAppendTableCreationEventualSuccess() throws Exception {
        ExecutionException noTable = new ExecutionException(
                new Throwable("Destination Table is deleted"));

        initialiseStreams();
        mockedStream.currentStreams.put(mockedTable1.toString(), "newStream");
        when(mockedApplicationStream1.areAllExpectedCallsCompleted()).thenReturn(true);
        when(mockedResponse.get()).thenThrow(noTable).thenReturn(successResponse);

        mockedStream.appendRows(mockedTable1, mockedRows, mockedStreamName1);

        verify(mockedStream, times(1)).attemptTableCreation(any(), any());
        verify(mockedApplicationStream1, times(1)).increaseCompletedCallsCount();
        verify(mockedApplicationStream1, times(1)).areAllExpectedCallsCompleted();
        verify(mockedApplicationStream1, times(1)).finalise();
        verify(mockedApplicationStream1, times(1)).commit();
    }

    @Test
    public void testAppendTableCreationEventualFail() throws Exception {
        ExecutionException noTable = new ExecutionException(
                new Throwable("Destination Table is deleted"));

        initialiseStreams();
        when(mockedResponse.get()).thenThrow(noTable);

        verifyException(exceeded30AttemptException);

        verify(mockedStream, times(1)).attemptTableCreation(any(), any());
        verify(mockedApplicationStream1, times(0)).increaseCompletedCallsCount();
    }

    @Test
    public void testAppendNonRetriable() throws Exception {
        InterruptedException nonRetriableException = new InterruptedException("I am a non-retriable error");

        initialiseStreams();
        when(mockedResponse.get()).thenThrow(nonRetriableException);

        verifyException(baseErrorMessage + "I am a non-retriable error");
    }

    @Test
    public void testAppendRetriable() throws Exception {
        ExecutionException exception = new ExecutionException(new StatusRuntimeException(
                io.grpc.Status.fromCode(io.grpc.Status.Code.INTERNAL).withDescription("I am an INTERNAL error")
        ));

        initialiseStreams();
        when(mockedResponse.get()).thenThrow(exception);

        verifyException(exceeded0AttemptException);
    }

    @Test
    public void testAppendStorageNonRetriable() throws Exception {
        ExecutionException exception = new ExecutionException(new StatusRuntimeException(
                io.grpc.Status.fromThrowable(new Throwable())
                        .withDescription("STREAM_FINALISED")
        ));

        initialiseStreams();
        when(mockedResponse.get()).thenThrow(exception);

        verifyException(baseErrorMessage + exception.getMessage());
    }

    @Test
    public void testSendAllToDLQ() throws Exception {
        ArgumentCaptor<Set<SinkRecord>> captorRecord = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Exception> captorException = ArgumentCaptor.forClass(Exception.class);

        initialiseStreams();
        when(mockedResponse.get()).thenThrow(badRecordsException);

        mockedStream.appendRows(mockedTable1, mockedRows, mockedStreamName1);

        verify(mockedErrantRecordHandler, times(1))
                .sendRecordsToDLQ(captorRecord.capture(), captorException.capture());
        assertTrue(captorRecord.getValue().contains(mockedSinkRecord));
        assertEquals(malformedExceptionMessage, captorException.getValue().getMessage());
        verify(mockedApplicationStream1, times(1)).increaseCompletedCallsCount();
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testSendSomeToDLQ() throws Exception {
        List<Object[]> rows = new ArrayList<>();
        ArgumentCaptor<Set<SinkRecord>> captorRecord = ArgumentCaptor.forClass(Set.class);

        initialiseStreams();
        rows.add(new Object[]{mockedSinkRecord, new JSONObject()});
        rows.add(new Object[]{mockedSinkRecord, new JSONObject()});
        when(mockedResponse.get()).thenThrow(badRecordsException).thenReturn(successResponse);

        mockedStream.appendRows(mockedTable1, rows, mockedStreamName1);

        verify(mockedErrantRecordHandler, times(1))
                .sendRecordsToDLQ(captorRecord.capture(), any());
        assertTrue(captorRecord.getValue().contains(mockedSinkRecord));
        verify(mockedApplicationStream1, times(1)).increaseCompletedCallsCount();
    }

    @Test
    public void testSendNoToDLQ() throws Exception {
        initialiseStreams();
        when(mockedResponse.get()).thenThrow(badRecordsException);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(null);

        verifyException(malformedExceptionMessage);
    }
}
