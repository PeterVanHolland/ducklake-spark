package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.connector.distributions.*;
import org.apache.spark.sql.connector.expressions.*;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.ConcurrentModificationException;

/**
 * Manages the lifecycle of a batch write operation to a DuckLake table.
 * Creates writer factories for executors and commits file metadata to the catalog.
 * Implements optimistic concurrency control to prevent conflicting concurrent writes.
 */
public class DuckLakeBatchWrite implements Write, BatchWrite, RequiresDistributionAndOrdering {

    private final CaseInsensitiveStringMap options;
    private final StructType schema;
    private final TableInfo tableInfo;
    private final List<ColumnInfo> columns;
    private final String dataPath;
    private final String tablePath;
    private final long[] columnIds;
    private final boolean isOverwrite;
    private final List<DuckLakeMetadataBackend.PartitionInfo> partitionInfos;
    private final long startingSnapshotId;

    public DuckLakeBatchWrite(CaseInsensitiveStringMap options, StructType schema,
                               TableInfo tableInfo, List<ColumnInfo> columns,
                               String dataPath, String tablePath,
                               long[] columnIds, boolean isOverwrite) {
        this(options, schema, tableInfo, columns, dataPath, tablePath, columnIds, isOverwrite, null);
    }

    public DuckLakeBatchWrite(CaseInsensitiveStringMap options, StructType schema,
                               TableInfo tableInfo, List<ColumnInfo> columns,
                               String dataPath, String tablePath,
                               long[] columnIds, boolean isOverwrite,
                               List<DuckLakeMetadataBackend.PartitionInfo> partitionInfos) {
        this.options = options;
        this.schema = schema;
        this.tableInfo = tableInfo;
        this.columns = columns;
        this.dataPath = dataPath;
        this.tablePath = tablePath;
        this.columnIds = columnIds;
        this.isOverwrite = isOverwrite;
        this.partitionInfos = partitionInfos;

        // Defer snapshot ID lookup to commit time (avoids extra connection open/close)
        this.startingSnapshotId = -1; // resolved lazily in commitWithOCC
    }

    @Override
    public BatchWrite toBatch() {
        return this;
    }


    @Override
    public StreamingWrite toStreaming() {
        return new DuckLakeStreamingWrite(options, schema, tableInfo, columns,
                dataPath, tablePath, columnIds);
    }
    @Override
    public Distribution requiredDistribution() {
        if (partitionInfos != null && !partitionInfos.isEmpty()) {
            // Cluster data by partition columns so each writer task gets
            // rows for a single partition value combination
            NamedReference[] refs = new NamedReference[partitionInfos.size()];
            for (int i = 0; i < partitionInfos.size(); i++) {
                final String colName = partitionInfos.get(i).columnName;
                refs[i] = Expressions.column(colName);
            }
            return Distributions.clustered(refs);
        }
        return Distributions.unspecified();
    }

    @Override
    public SortOrder[] requiredOrdering() {
        return new SortOrder[0];
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        // TODO: Add constraint validation here
        // Load constraints from metadata backend and validate against schema
        // For NOT NULL constraints: check if nullable columns have NOT NULL constraint
        // For UNIQUE constraints: this would require coordination across partitions

        String writeBasePath = dataPath + tablePath;
        return new DuckLakeDataWriterFactory(schema, columnIds, writeBasePath, tablePath, partitionInfos);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        List<DuckLakeWriterCommitMessage> validMessages = new ArrayList<>();
        for (WriterCommitMessage msg : messages) {
            if (msg == null) continue;
            if (msg instanceof DuckLakePartitionedWriterCommitMessage) {
                // Partitioned write: unwrap individual partition messages
                DuckLakePartitionedWriterCommitMessage partMsg = (DuckLakePartitionedWriterCommitMessage) msg;
                for (DuckLakeWriterCommitMessage pm : partMsg.partitionMessages) {
                    if (pm.recordCount > 0) {
                        validMessages.add(pm);
                    }
                }
            } else if (msg instanceof DuckLakeWriterCommitMessage) {
                DuckLakeWriterCommitMessage dlMsg = (DuckLakeWriterCommitMessage) msg;
                if (dlMsg.recordCount > 0) {
                    validMessages.add(dlMsg);
                }
            }
        }

        if (validMessages.isEmpty() && !isOverwrite) {
            return;
        }

        // Get max retry count from options (default 3)
        int maxRetries = Integer.parseInt(options.getOrDefault("occ.maxRetries", "3"));
        int retryCount = 0;

        while (retryCount <= maxRetries) {
            try {
                commitWithOCC(validMessages);
                return; // Success
            } catch (ConcurrentModificationException e) {
                retryCount++;
                if (retryCount > maxRetries) {
                    throw new RuntimeException("Failed to commit after " + maxRetries +
                            " retries due to concurrent modifications. Table: " + tableInfo.name, e);
                }

                // Brief pause before retry (exponential backoff)
                try {
                    Thread.sleep(100L * (1L << Math.min(retryCount - 1, 4))); // 100ms, 200ms, 400ms, 800ms, 1600ms max
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Write interrupted during retry", ie);
                }
            }
        }
    }

    /**
     * Performs the actual commit with optimistic concurrency control.
     * Throws ConcurrentModificationException if a conflict is detected.
     */
    private void commitWithOCC(List<DuckLakeWriterCommitMessage> validMessages)
            throws ConcurrentModificationException {
        try (DuckLakeMetadataBackend backend = createBackend()) {
            backend.beginTransaction();
            try {
                // Resolve current snapshot ID (deferred from constructor)
                long currentSnap = backend.getCurrentSnapshotId();
                CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);

                long newSnap = currentSnap + 1;
                long nextFileId = snapInfo.nextFileId;
                long newNextFileId = nextFileId + validMessages.size();

                // Create new snapshot atomically with OCC
                backend.createSnapshotAtomically(currentSnap, newSnap, snapInfo.schemaVersion,
                        snapInfo.nextCatalogId, newNextFileId);

                // If overwrite, mark existing files as deleted
                if (isOverwrite) {
                    backend.markDataFilesDeleted(tableInfo.tableId, newSnap);
                }

                // Get current table stats
                TableStats tableStats = backend.getTableStats(tableInfo.tableId);
                long rowIdStart = isOverwrite ? 0 : tableStats.nextRowId;
                long totalRecordCount = isOverwrite ? 0 : tableStats.recordCount;
                long totalFileSize = isOverwrite ? 0 : tableStats.fileSizeBytes;

                // Insert data files and batch column stats
                long fileId = nextFileId;
                int fileOrder = 0;
                for (DuckLakeWriterCommitMessage msg : validMessages) {
                    backend.insertDataFile(fileId, tableInfo.tableId, newSnap, fileOrder,
                            msg.relativePath, msg.recordCount, msg.fileSize, rowIdStart);

                    // Batch column stats for this file
                    if (!msg.columnStats.isEmpty()) {
                        List<long[]> statsData = new ArrayList<>();
                        List<String[]> statsStrings = new ArrayList<>();
                        for (DuckLakeWriterCommitMessage.ColumnStats stats : msg.columnStats) {
                            statsData.add(new long[]{stats.columnId, stats.valueCount, stats.nullCount});
                            statsStrings.add(new String[]{stats.minValue, stats.maxValue});
                        }
                        backend.insertColumnStatsBatch(fileId, tableInfo.tableId, statsData, statsStrings);
                    }

                    // Insert partition values if present
                    if (msg.partitionValues != null && !msg.partitionValues.isEmpty()) {
                        backend.insertPartitionValues(fileId, tableInfo.tableId, msg.partitionValues);
                    }

                    rowIdStart += msg.recordCount;
                    totalRecordCount += msg.recordCount;
                    totalFileSize += msg.fileSize;
                    fileId++;
                    fileOrder++;
                }

                // Update table stats
                backend.updateTableStats(tableInfo.tableId, totalRecordCount, rowIdStart, totalFileSize);

                // Record snapshot changes
                String changes = "inserted_into_table:" + tableInfo.tableId;
                if (isOverwrite) {
                    changes = "deleted_from_table:" + tableInfo.tableId + "," + changes;
                }
                backend.insertSnapshotChanges(newSnap, changes, "ducklake-spark", "Spark write");

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
            throw new RuntimeException("Failed to commit DuckLake write", e);
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
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
