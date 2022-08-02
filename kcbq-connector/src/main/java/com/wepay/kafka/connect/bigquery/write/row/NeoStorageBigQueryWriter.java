package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.*;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class NeoStorageBigQueryWriter extends BigQueryWriter {
  private static final Logger logger = LoggerFactory.getLogger(NeoStorageBigQueryWriter.class);

  private final BigQuery bigQuery;

  public NeoStorageBigQueryWriter(BigQuery bigQuery, int retry, long retryWait) {
    super(retry, retryWait);
    this.bigQuery = bigQuery;
  }

  @Override
  public Map<Long, List<BigQueryError>> performWriteRequest(
      PartitionedTableId tableId, SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows) {
    InsertAllRequest request = createInsertAllRequest(tableId, rows.values());

    logger.debug("RowToInsert");
    JSONArray arr = new JSONArray();
    for (InsertAllRequest.RowToInsert row : request.getRows()) {
      JSONObject obj = new JSONObject(row.getContent());
      arr.put(obj);
      logger.debug(obj.toString());
    }

    try {
      WriteToDefaultStream.writeToDefaultStream("", "", "", arr);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new HashMap<>();
  }
}
