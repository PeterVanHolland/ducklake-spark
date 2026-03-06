package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

/**
 * Manages the lifecycle of a batch write operation to a DuckLake table.
 * Creates writer factories for executors and commits file metadata to the catalog.
 */
public class DuckLakeBatchWrite implements Write, BatchWrite {

    private final CaseInsensitiveStringMap options;
    private final StructType schema;
    private final TableInfo tableInfo;
    private final List<ColumnInfo> columns;
    private final String dataPath;
    private final String tablePath;
    private final long[] columnIds;
    private final boolean isOverwrite;

    public DuckLakeBatchWrite(CaseInsensitiveStringMap options, StructType schema,
                               TableInfo tableInfo, List<ColumnInfo> columns,
                               String dataPath, String tablePath,
                               long[] columnIds, boolean isOverwrite) {
        this.options = options;
        this.schema = schema;
        this.tableInfo = tableInfo;
        this.columns = columns;
        this.dataPath = dataPath;
        this.tablePath = tablePath;
        this.columnIds = columnIds;
        this.isOverwrite = isOverwrite;
    }

    @Override
    public BatchWrite toBatch() {
        return this;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        String writeBasePath = dataPath + tablePath;
        return new DuckLakeDataWriterFactory(schema, columnIds, writeBasePath, tablePath);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        List<DuckLakeWriterCommitMessage> validMessages = new ArrayList<>();
        for (WriterCommitMessage msg : messages) {
            if (msg != null) {
                DuckLakeWriterCommitMessage dlMsg = (DuckLakeWriterCommitMessage) msg;
                if (dlMsg.recordCount > 0) {
                    validMessages.add(dlMsg);
                }
            }
        }

        if (validMessages.isEmpty() && !isOverwrite) {
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

                // Create new snapshot
                backend.createSnapshot(newSnap, snapInfo.schemaVersion,
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
