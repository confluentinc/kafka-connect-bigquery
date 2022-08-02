package com.wepay.kafka.connect.bigquery.write.row;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.grpc.Status;
import io.grpc.Status.Code;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.concurrent.Phaser;

public class WriteToDefaultStream {

  private static final Logger logger = LoggerFactory.getLogger(WriteToDefaultStream.class);


  public static void writeToDefaultStream(
      String projectId, String datasetName, String tableName, JSONArray jsonArr)
      throws DescriptorValidationException, InterruptedException, IOException {

    TableName parentTable = TableName.of(projectId, datasetName, tableName);

    DataWriter writer = new DataWriter();
    writer.initialize(parentTable);

    writer.append(new AppendContext(jsonArr, 0));

    // Final cleanup for the stream during worker teardown.
    writer.cleanup();
    System.out.println("Appended records successfully.");
  }

  private static class AppendContext {

    JSONArray data;
    int retryCount = 0;

    AppendContext(JSONArray data, int retryCount) {
      this.data = data;
      this.retryCount = retryCount;
    }
  }

  private static class DataWriter {

    private static final int MAX_RETRY_COUNT = 2;
    private static final ImmutableList<Code> RETRIABLE_ERROR_CODES =
        ImmutableList.of(Code.INTERNAL, Code.ABORTED, Code.CANCELLED);

    // Track the number of in-flight requests to wait for all responses before shutting down.
    private final Phaser inflightRequestCount = new Phaser(1);
    private final Object lock = new Object();
    private JsonStreamWriter streamWriter;



    @GuardedBy("lock")
    private RuntimeException error = null;

    public void initialize(TableName parentTable)
        throws DescriptorValidationException, IOException, InterruptedException {
      BigQuery bigquery = BigQueryOptions.newBuilder().build().getService();
      Table table = bigquery.getTable(parentTable.getDataset(), parentTable.getTable());
      Schema schema = table.getDefinition().getSchema();
      logger.debug(">>> bigqueryoptions debug");
      logger.debug(schema.toString());

      TableSchema tableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema);
      streamWriter = JsonStreamWriter.newBuilder(parentTable.toString(), tableSchema).build();
    }

    public void append(AppendContext appendContext)
        throws DescriptorValidationException, IOException {
      synchronized (this.lock) {
        // If earlier appends have failed, we need to reset before continuing.
        if (this.error != null) {
          throw this.error;
        }
      }
      // Append asynchronously for increased throughput.
      ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
      ApiFutures.addCallback(
          future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

      // Increase the count of in-flight requests.
      inflightRequestCount.register();
    }

    public void cleanup() {
      // Wait for all in-flight requests to complete.
      inflightRequestCount.arriveAndAwaitAdvance();

      // Close the connection to the server.
      streamWriter.close();

      // Verify that no error occurred in the stream.
      synchronized (this.lock) {
        if (this.error != null) {
          throw this.error;
        }
      }
    }

    static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

      private final DataWriter parent;
      private final AppendContext appendContext;

      public AppendCompleteCallback(DataWriter parent, AppendContext appendContext) {
        this.parent = parent;
        this.appendContext = appendContext;
      }

      public void onSuccess(AppendRowsResponse response) {
        logger.debug("Append success");
        done();
      }

      public void onFailure(Throwable throwable) {
        // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
        // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
        // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
        Status status = Status.fromThrowable(throwable);
        if (appendContext.retryCount < MAX_RETRY_COUNT
            && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
          appendContext.retryCount++;
          try {
            // Since default stream appends are not ordered, we can simply retry the appends.
            // Retrying with exclusive streams requires more careful consideration.
            this.parent.append(appendContext);
            // Mark the existing attempt as done since it's being retried.
            done();
            return;
          } catch (Exception e) {
            // Fall through to return error.
            System.out.format("Failed to retry append: %s\n", e);
          }
        }

        synchronized (this.parent.lock) {
          if (this.parent.error == null) {
            StorageException storageException = Exceptions.toStorageException(throwable);
            this.parent.error =
                (storageException != null) ? storageException : new RuntimeException(throwable);
          }
        }
        System.out.format("Error: %s\n", throwable);
        done();
      }

      private void done() {
        // Reduce the count of in-flight requests.
        this.parent.inflightRequestCount.arriveAndDeregister();
      }
    }
  }
}
