package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.PartitionInfo;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.*;

/**
 * Partitioning-aware data writer that splits incoming rows into
 * separate Parquet files per unique partition value combination.
 *
 * When the table is NOT partitioned, delegates directly to a single
 * DuckLakeDataWriter. When partitioned, creates a new sub-writer
 * for each new combination of partition column values.
 */
public class DuckLakePartitioningDataWriter implements DataWriter<InternalRow> {

    private final StructType schema;
    private final long[] columnIds;
    private final String writeBasePath;
    private final String tablePath;
    private final int sparkPartitionId;
    private final long taskAttemptId;
    private final List<PartitionInfo> partitionInfos;

    // Partition column indices in the schema
    private final int[] partColFieldIndices;

    // One sub-writer per unique partition value combination
    private final Map<String, DuckLakeDataWriter> writers = new LinkedHashMap<>();
    private final Map<String, Map<Integer, String>> writerPartitionValues = new LinkedHashMap<>();

    public DuckLakePartitioningDataWriter(StructType schema, long[] columnIds,
                                           String writeBasePath, String tablePath,
                                           int sparkPartitionId, long taskAttemptId,
                                           List<PartitionInfo> partitionInfos) {
        this.schema = schema;
        this.columnIds = columnIds;
        this.writeBasePath = writeBasePath;
        this.tablePath = tablePath;
        this.sparkPartitionId = sparkPartitionId;
        this.taskAttemptId = taskAttemptId;
        this.partitionInfos = partitionInfos;

        // Map partition info to schema field indices
        if (partitionInfos != null && !partitionInfos.isEmpty()) {
            partColFieldIndices = new int[partitionInfos.size()];
            for (int pi = 0; pi < partitionInfos.size(); pi++) {
                PartitionInfo info = partitionInfos.get(pi);
                partColFieldIndices[pi] = -1;
                for (int fi = 0; fi < schema.fields().length; fi++) {
                    if (schema.fields()[fi].name().equalsIgnoreCase(info.columnName)) {
                        partColFieldIndices[pi] = fi;
                        break;
                    }
                }
            }
        } else {
            partColFieldIndices = null;
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        if (partColFieldIndices == null || partColFieldIndices.length == 0) {
            // Non-partitioned: single writer
            DuckLakeDataWriter writer = writers.get("");
            if (writer == null) {
                writer = new DuckLakeDataWriter(schema, columnIds, writeBasePath,
                        tablePath, sparkPartitionId, taskAttemptId);
                writers.put("", writer);
            }
            writer.write(row);
            return;
        }

        // Extract partition key
        StringBuilder keyBuilder = new StringBuilder();
        Map<Integer, String> partVals = new HashMap<>();
        for (int i = 0; i < partColFieldIndices.length; i++) {
            int fieldIdx = partColFieldIndices[i];
            String val;
            if (fieldIdx < 0 || row.isNullAt(fieldIdx)) {
                val = "__NULL__";
            } else {
                DataType type = schema.fields()[fieldIdx].dataType();
                val = extractStringValue(row, fieldIdx, type);
            }
            partVals.put(partitionInfos.get(i).partitionIndex, val);
            if (i > 0) keyBuilder.append('|');
            keyBuilder.append(val);
        }
        String key = keyBuilder.toString();

        DuckLakeDataWriter writer = writers.get(key);
        if (writer == null) {
            // Create new sub-writer without partition tracking
            // (we track partition values ourselves here)
            writer = new DuckLakeDataWriter(schema, columnIds, writeBasePath,
                    tablePath, sparkPartitionId, taskAttemptId);
            writers.put(key, writer);
            writerPartitionValues.put(key, partVals);
        }
        writer.write(row);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if (writers.size() == 1 && writers.containsKey("")) {
            // Non-partitioned: return single message
            return writers.get("").commit();
        }

        // Partitioned: collect messages with partition values
        List<DuckLakeWriterCommitMessage> messages = new ArrayList<>();
        for (Map.Entry<String, DuckLakeDataWriter> entry : writers.entrySet()) {
            DuckLakeWriterCommitMessage msg = (DuckLakeWriterCommitMessage) entry.getValue().commit();
            // Override partition values with our tracked values
            Map<Integer, String> pv = writerPartitionValues.get(entry.getKey());
            if (pv != null) {
                // The msg already has partitionValues from the sub-writer,
                // but it might be empty since we didn't pass PartitionInfos.
                // We need to set it via a wrapper.
                messages.add(new DuckLakeWriterCommitMessage(
                        msg.absolutePath, msg.relativePath,
                        msg.recordCount, msg.fileSize,
                        msg.columnStats, pv));
            } else {
                messages.add(msg);
            }
        }

        // Return a composite message
        return new DuckLakePartitionedWriterCommitMessage(messages);
    }

    @Override
    public void abort() throws IOException {
        for (DuckLakeDataWriter writer : writers.values()) {
            writer.abort();
        }
    }

    @Override
    public void close() throws IOException {
        for (DuckLakeDataWriter writer : writers.values()) {
            writer.close();
        }
    }

    private String extractStringValue(InternalRow row, int ordinal, DataType type) {
        if (type instanceof IntegerType) return String.valueOf(row.getInt(ordinal));
        if (type instanceof LongType) return String.valueOf(row.getLong(ordinal));
        if (type instanceof StringType) return row.getUTF8String(ordinal).toString();
        if (type instanceof DoubleType) return String.valueOf(row.getDouble(ordinal));
        if (type instanceof FloatType) return String.valueOf(row.getFloat(ordinal));
        if (type instanceof BooleanType) return String.valueOf(row.getBoolean(ordinal));
        if (type instanceof ShortType) return String.valueOf(row.getShort(ordinal));
        if (type instanceof ByteType) return String.valueOf(row.getByte(ordinal));
        return row.get(ordinal, type).toString();
    }
}
