package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeCatalog;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.util.DuckLakeTypeMapping;

import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.*;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

/**
 * Executes row-level DELETE operations on DuckLake tables.
 *
 * Scans all data files, evaluates the WHERE predicate against each row,
 * and writes delete files for matching rows. Delete files are Parquet files
 * with a single {@code row_id} column indicating which row positions to exclude.
 */
public class DuckLakeDeleteExecutor {

    private final String catalogPath;
    private final String dataPathOption;
    private final String tableName;
    private final String schemaName;

    public DuckLakeDeleteExecutor(String catalogPath, String dataPathOption,
                                   String tableName, String schemaName) {
        this.catalogPath = catalogPath;
        this.dataPathOption = dataPathOption;
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    /**
     * Delete all rows matching the given filters.
     * Creates delete files and registers them in the catalog.
     */
    public void deleteWhere(Filter[] filters) {
        try (DuckLakeMetadataBackend backend = createBackend()) {
            backend.beginTransaction();
            try {
                long currentSnap = backend.getCurrentSnapshotId();

                TableInfo table = backend.getTable(tableName, schemaName, currentSnap);
                if (table == null) {
                    throw new RuntimeException("Table not found: " + schemaName + "." + tableName);
                }

                String dataPath = backend.getDataPath();
                if (!dataPath.endsWith("/")) dataPath += "/";

                List<DataFileInfo> dataFiles = backend.getDataFiles(table.tableId, currentSnap);
                List<ColumnInfo> columns = backend.getColumns(table.tableId, currentSnap);

                StructType sparkSchema = DuckLakeTypeMapping.buildSchema(columns);

                Map<Long, String> colIdToName = new HashMap<>();
                for (ColumnInfo col : columns) {
                    colIdToName.put(col.columnId, col.name);
                }

                CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);
                long nextFileId = snapInfo.nextFileId;

                List<DeleteFileResult> deleteResults = new ArrayList<>();

                for (DataFileInfo dataFile : dataFiles) {
                    String filePath = dataFile.pathIsRelative ? dataPath + dataFile.path : dataFile.path;

                    // Load existing delete positions for this data file
                    Set<Long> existingDeletes = loadExistingDeletes(
                            backend, table.tableId, dataFile.dataFileId, currentSnap, dataPath);

                    // Build logical-to-physical column name mapping
                    Map<String, String> logicalToPhysical = buildLogicalToPhysical(
                            backend, dataFile.mappingId, colIdToName);

                    // Scan file and find matching row positions
                    List<Long> matchingPositions = scanForMatches(
                            filePath, sparkSchema, filters, existingDeletes, logicalToPhysical);

                    if (!matchingPositions.isEmpty()) {
                        String deleteFileName = "delete_" + UUID.randomUUID() + ".parquet";
                        String deleteRelPath = table.path + deleteFileName;
                        String deleteAbsPath = dataPath + deleteRelPath;

                        writeDeleteFile(deleteAbsPath, matchingPositions);

                        long fileSize = new File(deleteAbsPath).length();
                        deleteResults.add(new DeleteFileResult(
                                dataFile.dataFileId, deleteRelPath, matchingPositions.size(), fileSize));
                    }
                }

                if (deleteResults.isEmpty()) {
                    backend.rollbackTransaction();
                    return;
                }

                // Create new snapshot
                long newSnap = currentSnap + 1;
                long newNextFileId = nextFileId + deleteResults.size();
                backend.createSnapshot(newSnap, snapInfo.schemaVersion,
                        snapInfo.nextCatalogId, newNextFileId);

                // Register delete files
                long deleteFileId = nextFileId;
                long totalDeleteCount = 0;
                for (DeleteFileResult result : deleteResults) {
                    backend.insertDeleteFile(deleteFileId, table.tableId, newSnap,
                            result.dataFileId, result.relativePath, result.deleteCount, result.fileSize);
                    totalDeleteCount += result.deleteCount;
                    deleteFileId++;
                }

                // Update table stats
                TableStats stats = backend.getTableStats(table.tableId);
                backend.updateTableStats(table.tableId,
                        stats.recordCount - totalDeleteCount, stats.nextRowId, stats.fileSizeBytes);

                // Record changes
                backend.insertSnapshotChanges(newSnap,
                        "deleted_from_table:" + table.tableId,
                        "ducklake-spark", "Spark delete");

                backend.commitTransaction();
            } catch (Exception e) {
                try { backend.rollbackTransaction(); } catch (SQLException ex) { e.addSuppressed(ex); }
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException("Failed to execute delete", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute delete", e);
        }
    }

    // ---------------------------------------------------------------
    // Scanning and matching
    // ---------------------------------------------------------------

    private List<Long> scanForMatches(String filePath, StructType sparkSchema,
                                       Filter[] filters, Set<Long> existingDeletes,
                                       Map<String, String> logicalToPhysical) {
        List<Long> matchingPositions = new ArrayList<>();

        try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(filePath))) {
            MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
            DuckLakeRowFilterEvaluator evaluator =
                    new DuckLakeRowFilterEvaluator(sparkSchema, fileSchema, logicalToPhysical);

            long rowPosition = 0;
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rowCount = pages.getRowCount();
                ColumnIOFactory factory = new ColumnIOFactory();
                MessageColumnIO columnIO = factory.getColumnIO(fileSchema);
                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(fileSchema));

                for (long i = 0; i < rowCount; i++) {
                    Group group = recordReader.read();
                    long pos = rowPosition++;

                    // Skip already-deleted rows
                    if (existingDeletes.contains(pos)) {
                        continue;
                    }

                    if (evaluator.matches(group, filters)) {
                        matchingPositions.add(pos);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan file for delete: " + filePath, e);
        }

        return matchingPositions;
    }

    // ---------------------------------------------------------------
    // Delete file writing
    // ---------------------------------------------------------------

    /**
     * Write a delete file (Parquet) with a single {@code row_id} column.
     */
    public static void writeDeleteFile(String absolutePath, List<Long> rowIds) {
        MessageType deleteSchema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("row_id")
                .named("delete_file");
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(deleteSchema);

        File parentDir = new File(absolutePath).getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(absolutePath))
                .withType(deleteSchema)
                .withConf(new Configuration())
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            for (long rowId : rowIds) {
                Group group = groupFactory.newGroup();
                group.add(0, rowId);
                writer.write(group);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write delete file: " + absolutePath, e);
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private Set<Long> loadExistingDeletes(DuckLakeMetadataBackend backend, long tableId,
                                           long dataFileId, long snapshotId,
                                           String dataPath) throws SQLException {
        Set<Long> deleted = new HashSet<>();
        List<DeleteFileInfo> deleteFiles = backend.getDeleteFiles(tableId, dataFileId, snapshotId);

        for (DeleteFileInfo df : deleteFiles) {
            String path = df.pathIsRelative ? dataPath + df.path : df.path;
            try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(path))) {
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    ColumnIOFactory factory = new ColumnIOFactory();
                    MessageColumnIO columnIO = factory.getColumnIO(schema);
                    RecordReader<Group> recordReader =
                            columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (long i = 0; i < pages.getRowCount(); i++) {
                        Group group = recordReader.read();
                        int fieldIndex = schema.getFieldIndex("row_id");
                        deleted.add(group.getLong(fieldIndex, 0));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read delete file: " + path, e);
            }
        }

        return deleted;
    }

    private Map<String, String> buildLogicalToPhysical(DuckLakeMetadataBackend backend,
                                                         long mappingId,
                                                         Map<Long, String> colIdToName) throws SQLException {
        Map<String, String> logicalToPhysical = new HashMap<>();
        if (mappingId >= 0) {
            Map<Long, String> nameMapping = backend.getNameMapping(mappingId);
            for (Map.Entry<Long, String> entry : nameMapping.entrySet()) {
                long fieldId = entry.getKey();
                String physicalName = entry.getValue();
                String logicalName = colIdToName.get(fieldId);
                if (logicalName != null && !physicalName.equals(logicalName)) {
                    logicalToPhysical.put(logicalName, physicalName);
                }
            }
        }
        return logicalToPhysical;
    }

    private DuckLakeMetadataBackend createBackend() {
        return new DuckLakeMetadataBackend(catalogPath, dataPathOption);
    }

    // ---------------------------------------------------------------
    // Result data class
    // ---------------------------------------------------------------

    static class DeleteFileResult {
        final long dataFileId;
        final String relativePath;
        final long deleteCount;
        final long fileSize;

        DeleteFileResult(long dataFileId, String relativePath, long deleteCount, long fileSize) {
            this.dataFileId = dataFileId;
            this.relativePath = relativePath;
            this.deleteCount = deleteCount;
            this.fileSize = fileSize;
        }
    }
}
