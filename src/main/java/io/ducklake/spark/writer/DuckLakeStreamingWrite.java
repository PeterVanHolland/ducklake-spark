package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

/**
 * Manages the lifecycle of a streaming write operation to a DuckLake table.
 * Each micro-batch (epochId) creates its own snapshot in the catalog.
 * Exactly-once semantics are provided by Spark's checkpoint mechanism —
 * the streaming engine tracks committed epochs and will not re-commit
 * an already-processed epoch.
 */
public class DuckLakeStreamingWrite implements StreamingWrite {

    private final CaseInsensitiveStringMap options;
    private final StructType schema;
    private final TableInfo tableInfo;
    private final List<ColumnInfo> columns;
    private final String dataPath;
    private final String tablePath;
    private final long[] columnIds;

    public DuckLakeStreamingWrite(CaseInsensitiveStringMap options, StructType schema,
                                   TableInfo tableInfo, List<ColumnInfo> columns,
                                   String dataPath, String tablePath, long[] columnIds) {
        this.options = options;
        this.schema = schema;
        this.tableInfo = tableInfo;
        this.columns = columns;
        this.dataPath = dataPath;
        this.tablePath = tablePath;
        this.columnIds = columnIds;
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        String writeBasePath = dataPath + tablePath;
        return new DuckLakeStreamWriterFactory(schema, columnIds, writeBasePath, tablePath);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        List<DuckLakeWriterCommitMessage> validMessages = new ArrayList<>();
        for (WriterCommitMessage msg : messages) {
            if (msg != null) {
                DuckLakeWriterCommitMessage dlMsg = (DuckLakeWriterCommitMessage) msg;
                if (dlMsg.recordCount > 0) {
                    validMessages.add(dlMsg);
                }
            }
        }

        if (validMessages.isEmpty()) {
            return;
        }

        try (DuckLakeMetadataBackend backend = createBackend()) {
            backend.beginTransaction();
            try {
                long currentSnap = backend.getCurrentSnapshotId();
                CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);

                long newSnap = currentSnap + 1;
                long nextFileId = snapInfo.nextFileId;
                long newNextFileId = nextFileId + validMessages.size();

                // Create new snapshot for this micro-batch
                backend.createSnapshot(newSnap, snapInfo.schemaVersion,
                        snapInfo.nextCatalogId, newNextFileId);

                // Get current table stats
                TableStats tableStats = backend.getTableStats(tableInfo.tableId);
                long rowIdStart = tableStats.nextRowId;
                long totalRecordCount = tableStats.recordCount;
                long totalFileSize = tableStats.fileSizeBytes;

                // Insert data files and column stats
                long fileId = nextFileId;
                int fileOrder = 0;
                for (DuckLakeWriterCommitMessage msg : validMessages) {
                    backend.insertDataFile(fileId, tableInfo.tableId, newSnap, fileOrder,
                            msg.relativePath, msg.recordCount, msg.fileSize, rowIdStart);

                    for (DuckLakeWriterCommitMessage.ColumnStats stats : msg.columnStats) {
                        backend.insertColumnStats(fileId, tableInfo.tableId, stats.columnId,
                                stats.valueCount, stats.nullCount, stats.minValue, stats.maxValue);
                    }

                    rowIdStart += msg.recordCount;
                    totalRecordCount += msg.recordCount;
                    totalFileSize += msg.fileSize;
                    fileId++;
                    fileOrder++;
                }

                // Update table stats
                backend.updateTableStats(tableInfo.tableId, totalRecordCount, rowIdStart, totalFileSize);

                // Record snapshot changes with epoch info
                String changes = "inserted_into_table:" + tableInfo.tableId;
                backend.insertSnapshotChanges(newSnap, changes, "ducklake-spark",
                        "Streaming write epoch " + epochId);

                backend.commitTransaction();
            } catch (Exception e) {
                try {
                    backend.rollbackTransaction();
                } catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to commit streaming write for epoch " + epochId, e);
        }
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        for (WriterCommitMessage msg : messages) {
            if (msg != null) {
                DuckLakeWriterCommitMessage dlMsg = (DuckLakeWriterCommitMessage) msg;
                try {
                    new File(dlMsg.absolutePath).delete();
                } catch (Exception e) {
                    // Best effort cleanup
                }
            }
        }
    }

    private DuckLakeMetadataBackend createBackend() {
        String catalog = options.get("catalog");
        String dp = options.getOrDefault("data_path", null);
        return new DuckLakeMetadataBackend(catalog, dp);
    }
}
