package io.ducklake.spark.maintenance;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.util.DuckLakeTypeMapping;
import io.ducklake.spark.writer.DuckLakeDeleteExecutor;
import io.ducklake.spark.writer.DuckLakeRowFilterEvaluator;
import io.ducklake.spark.writer.DuckLakeWriterCommitMessage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

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
 * MERGE INTO support for DuckLake tables via Spark.
 *
 * <p>Provides a builder-pattern API for executing MERGE operations (upserts)
 * against DuckLake tables. This is analogous to the SQL MERGE INTO statement
 * and supports matched updates, matched deletes, and not-matched inserts.</p>
 *
 * <p>Usage:</p>
 * <pre>
 *   DuckLakeMerge.into(spark, catalogPath, "target_table", "main")
 *       .using(sourceDF, "source")
 *       .on("target.id = source.id")
 *       .whenMatched().updateAll()
 *       .whenNotMatched().insertAll()
 *       .execute();
 * </pre>
 *
 * <p>Implementation uses copy-on-write semantics: matched rows that are updated
 * produce delete files for old versions and new data files for updated versions.
 * All changes are committed in a single atomic snapshot.</p>
 */
public class DuckLakeMerge {

    private DuckLakeMerge() {} // utility class - use builder

    /**
     * Start building a MERGE INTO operation.
     *
     * @param spark       active SparkSession
     * @param catalogPath path to the .ducklake catalog file
     * @param tableName   name of the target table
     * @param schemaName  schema (namespace) containing the table
     * @return a MergeBuilder for fluent configuration
     */
    public static MergeBuilder into(SparkSession spark, String catalogPath,
                                     String tableName, String schemaName) {
        return new MergeBuilder(spark, catalogPath, tableName, schemaName);
    }

    // =================================================================
    // Builder classes
    // =================================================================

    /**
     * Fluent builder for configuring and executing a MERGE operation.
     */
    public static class MergeBuilder {
        private final SparkSession spark;
        private final String catalogPath;
        private final String tableName;
        private final String schemaName;
        private String dataPathOption;

        private Dataset<Row> sourceDF;
        private String sourceAlias;
        private String onCondition;

        private final List<MatchedClause> matchedClauses = new ArrayList<>();
        private final List<NotMatchedClause> notMatchedClauses = new ArrayList<>();

        MergeBuilder(SparkSession spark, String catalogPath,
                     String tableName, String schemaName) {
            this.spark = spark;
            this.catalogPath = catalogPath;
            this.tableName = tableName;
            this.schemaName = schemaName;
        }

        /**
         * Override the data path (useful for testing).
         * If not set, the data path is read from catalog metadata.
         */
        public MergeBuilder dataPath(String dataPath) {
            this.dataPathOption = dataPath;
            return this;
        }

        /**
         * Specify the source DataFrame and its alias for the ON condition.
         */
        public MergeBuilder using(Dataset<Row> source, String alias) {
            this.sourceDF = source;
            this.sourceAlias = alias;
            return this;
        }

        /**
         * Specify the join condition between target and source.
         * Supports equality conditions with optional AND:
         * <pre>"target.id = source.id"</pre>
         * <pre>"target.id = source.id AND target.region = source.region"</pre>
         */
        public MergeBuilder on(String condition) {
            this.onCondition = condition;
            return this;
        }

        /** Start defining what to do when a target row matches a source row. */
        public WhenMatchedBuilder whenMatched() {
            return new WhenMatchedBuilder(this, null);
        }

        /**
         * Start defining what to do when a target row matches a source row
         * AND the given additional condition is true.
         *
         * @param condition additional predicate, e.g. "source.value > 100"
         */
        public WhenMatchedBuilder whenMatched(String condition) {
            return new WhenMatchedBuilder(this, condition);
        }

        /** Start defining what to do when a source row has no matching target row. */
        public WhenNotMatchedBuilder whenNotMatched() {
            return new WhenNotMatchedBuilder(this, null);
        }

        /**
         * Start defining what to do when a source row has no matching target row
         * AND the given additional condition is true.
         */
        public WhenNotMatchedBuilder whenNotMatched(String condition) {
            return new WhenNotMatchedBuilder(this, condition);
        }

        /**
         * Execute the MERGE operation. All changes are committed atomically
         * in a single new snapshot.
         */
        public void execute() {
            if (sourceDF == null) {
                throw new IllegalStateException(
                        "Source DataFrame not set. Call using() before execute().");
            }
            if (onCondition == null || onCondition.isEmpty()) {
                throw new IllegalStateException(
                        "ON condition not set. Call on() before execute().");
            }
            if (matchedClauses.isEmpty() && notMatchedClauses.isEmpty()) {
                throw new IllegalStateException(
                        "No WHEN clauses defined. Add at least one whenMatched() or whenNotMatched() clause.");
            }

            new MergeExecutor(this).run();
        }
    }

    /** Builder for WHEN MATCHED actions. */
    public static class WhenMatchedBuilder {
        private final MergeBuilder parent;
        private final String condition;

        WhenMatchedBuilder(MergeBuilder parent, String condition) {
            this.parent = parent;
            this.condition = condition;
        }

        /** Update all target columns with values from the source row. */
        public MergeBuilder updateAll() {
            parent.matchedClauses.add(
                    new MatchedClause(condition, MatchAction.UPDATE_ALL, null));
            return parent;
        }

        /** Update specific target columns. Keys = target cols, values = source cols. */
        public MergeBuilder update(Map<String, String> assignments) {
            parent.matchedClauses.add(
                    new MatchedClause(condition, MatchAction.UPDATE, assignments));
            return parent;
        }

        /** Delete the matched target row. */
        public MergeBuilder delete() {
            parent.matchedClauses.add(
                    new MatchedClause(condition, MatchAction.DELETE, null));
            return parent;
        }
    }

    /** Builder for WHEN NOT MATCHED actions. */
    public static class WhenNotMatchedBuilder {
        private final MergeBuilder parent;
        private final String condition;

        WhenNotMatchedBuilder(MergeBuilder parent, String condition) {
            this.parent = parent;
            this.condition = condition;
        }

        /** Insert all columns from the unmatched source row. */
        public MergeBuilder insertAll() {
            parent.notMatchedClauses.add(
                    new NotMatchedClause(condition, InsertAction.INSERT_ALL, null));
            return parent;
        }

        /** Insert specific columns. Keys = target cols, values = source cols. */
        public MergeBuilder insert(Map<String, String> assignments) {
            parent.notMatchedClauses.add(
                    new NotMatchedClause(condition, InsertAction.INSERT, assignments));
            return parent;
        }
    }

    // =================================================================
    // Internal data structures
    // =================================================================

    enum MatchAction { UPDATE_ALL, UPDATE, DELETE }
    enum InsertAction { INSERT_ALL, INSERT }

    static class MatchedClause {
        final String condition;
        final MatchAction action;
        final Map<String, String> assignments;

        MatchedClause(String condition, MatchAction action,
                      Map<String, String> assignments) {
            this.condition = condition;
            this.action = action;
            this.assignments = assignments;
        }
    }

    static class NotMatchedClause {
        final String condition;
        final InsertAction action;
        final Map<String, String> assignments;

        NotMatchedClause(String condition, InsertAction action,
                         Map<String, String> assignments) {
            this.condition = condition;
            this.action = action;
            this.assignments = assignments;
        }
    }

    /** Parsed join key pair: target column <-> source column. */
    static class JoinKeyPair {
        final String targetColumn;
        final String sourceColumn;

        JoinKeyPair(String targetColumn, String sourceColumn) {
            this.targetColumn = targetColumn;
            this.sourceColumn = sourceColumn;
        }
    }

    // =================================================================
    // Merge Executor
    // =================================================================

    private static class MergeExecutor {
        private final MergeBuilder config;
        private final List<JoinKeyPair> joinKeys;

        MergeExecutor(MergeBuilder config) {
            this.config = config;
            this.joinKeys = parseOnCondition(config.onCondition, config.sourceAlias);
        }

        void run() {
            List<Row> sourceRows = config.sourceDF.collectAsList();
            if (sourceRows.isEmpty()) {
                return; // Nothing to merge
            }

            StructType sourceSchema = config.sourceDF.schema();
            Map<String, List<Integer>> sourceIndex =
                    buildSourceIndex(sourceRows, sourceSchema);

            try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(
                    config.catalogPath, config.dataPathOption)) {
                backend.beginTransaction();
                try {
                    executeMerge(backend, sourceRows, sourceSchema, sourceIndex);
                    backend.commitTransaction();
                } catch (Exception e) {
                    try {
                        backend.rollbackTransaction();
                    } catch (SQLException ex) {
                        e.addSuppressed(ex);
                    }
                    if (e instanceof RuntimeException) throw (RuntimeException) e;
                    throw new RuntimeException("Failed to execute merge", e);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to execute merge", e);
            }
        }

        private void executeMerge(DuckLakeMetadataBackend backend,
                                   List<Row> sourceRows, StructType sourceSchema,
                                   Map<String, List<Integer>> sourceIndex)
                throws SQLException {
            long currentSnap = backend.getCurrentSnapshotId();

            TableInfo table = backend.getTable(
                    config.tableName, config.schemaName, currentSnap);
            if (table == null) {
                throw new RuntimeException(
                        "Table not found: " + config.schemaName + "." + config.tableName);
            }

            String dataPath = backend.getDataPath();
            if (!dataPath.endsWith("/")) dataPath += "/";

            List<DataFileInfo> dataFiles =
                    backend.getDataFiles(table.tableId, currentSnap);
            List<ColumnInfo> columns =
                    backend.getColumns(table.tableId, currentSnap);

            StructType targetSchema = DuckLakeTypeMapping.buildSchema(columns);

            Map<Long, String> colIdToName = new HashMap<>();
            for (ColumnInfo col : columns) {
                colIdToName.put(col.columnId, col.name);
            }

            long[] columnIds = new long[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                columnIds[i] = columns.get(i).columnId;
            }

            CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);
            long nextFileId = snapInfo.nextFileId;

            boolean[] sourceMatched = new boolean[sourceRows.size()];

            List<DeleteFileResult> deleteResults = new ArrayList<>();
            List<NewDataFileResult> newDataFiles = new ArrayList<>();

            // --- Phase 1: scan target data files, match against source ---
            for (DataFileInfo dataFile : dataFiles) {
                String filePath = dataFile.pathIsRelative
                        ? dataPath + dataFile.path : dataFile.path;

                Set<Long> existingDeletes = loadExistingDeletes(
                        backend, table.tableId, dataFile.dataFileId,
                        currentSnap, dataPath);

                Map<String, String> logicalToPhysical = buildLogicalToPhysical(
                        backend, dataFile.mappingId, colIdToName);

                ScanResult scanResult = scanAndMerge(
                        filePath, targetSchema, sourceRows, sourceSchema,
                        sourceIndex, existingDeletes, logicalToPhysical,
                        columnIds, sourceMatched);

                if (!scanResult.deletePositions.isEmpty()) {
                    String deleteFileName = "delete_" + UUID.randomUUID() + ".parquet";
                    String deleteRelPath = table.path + deleteFileName;
                    String deleteAbsPath = dataPath + deleteRelPath;

                    DuckLakeDeleteExecutor.writeDeleteFile(
                            deleteAbsPath, scanResult.deletePositions);

                    long deleteFileSize = new File(deleteAbsPath).length();
                    deleteResults.add(new DeleteFileResult(
                            dataFile.dataFileId, deleteRelPath,
                            scanResult.deletePositions.size(), deleteFileSize));
                }

                if (!scanResult.updatedRows.isEmpty()) {
                    String newFileName = "ducklake-" + UUID.randomUUID() + ".parquet";
                    String newRelPath = table.path + newFileName;
                    String newAbsPath = dataPath + newRelPath;

                    writeDataRows(newAbsPath, targetSchema, columnIds,
                            scanResult.updatedRows);

                    long newFileSize = new File(newAbsPath).length();
                    newDataFiles.add(new NewDataFileResult(
                            newRelPath, scanResult.updatedRows.size(),
                            newFileSize, scanResult.updateColumnStats));
                }
            }

            // --- Phase 2: insert unmatched source rows ---
            List<Object[]> insertRows = collectInsertRows(
                    sourceRows, sourceSchema, sourceMatched,
                    targetSchema, columns);

            if (!insertRows.isEmpty()) {
                String insertFileName = "ducklake-" + UUID.randomUUID() + ".parquet";
                String insertRelPath = table.path + insertFileName;
                String insertAbsPath = dataPath + insertRelPath;

                List<DuckLakeWriterCommitMessage.ColumnStats> insertStats =
                        writeDataRowsWithStats(
                                insertAbsPath, targetSchema, columnIds, insertRows);

                long insertFileSize = new File(insertAbsPath).length();
                newDataFiles.add(new NewDataFileResult(
                        insertRelPath, insertRows.size(),
                        insertFileSize, insertStats));
            }

            // Nothing changed
            if (deleteResults.isEmpty() && newDataFiles.isEmpty()) {
                backend.rollbackTransaction();
                return;
            }

            // --- Phase 3: commit all changes atomically ---
            long newSnap = currentSnap + 1;
            long totalNewFiles = deleteResults.size() + newDataFiles.size();
            long newNextFileId = nextFileId + totalNewFiles;
            backend.createSnapshot(newSnap, snapInfo.schemaVersion,
                    snapInfo.nextCatalogId, newNextFileId);

            long fileId = nextFileId;
            long totalDeleteCount = 0;
            for (DeleteFileResult result : deleteResults) {
                backend.insertDeleteFile(fileId, table.tableId, newSnap,
                        result.dataFileId, result.relativePath,
                        result.deleteCount, result.fileSize);
                totalDeleteCount += result.deleteCount;
                fileId++;
            }

            TableStats stats = backend.getTableStats(table.tableId);
            long rowIdStart = stats.nextRowId;
            long totalNewRecords = 0;
            long totalNewFileSize = 0;
            int fileOrder = 0;

            for (NewDataFileResult ndf : newDataFiles) {
                backend.insertDataFile(fileId, table.tableId, newSnap,
                        fileOrder, ndf.relativePath, ndf.recordCount,
                        ndf.fileSize, rowIdStart);

                for (DuckLakeWriterCommitMessage.ColumnStats cs : ndf.columnStats) {
                    backend.insertColumnStats(fileId, table.tableId,
                            cs.columnId, cs.valueCount, cs.nullCount,
                            cs.minValue, cs.maxValue);
                }

                rowIdStart += ndf.recordCount;
                totalNewRecords += ndf.recordCount;
                totalNewFileSize += ndf.fileSize;
                fileId++;
                fileOrder++;
            }

            long newRecordCount =
                    stats.recordCount - totalDeleteCount + totalNewRecords;
            backend.updateTableStats(table.tableId, newRecordCount,
                    rowIdStart, stats.fileSizeBytes + totalNewFileSize);

            StringBuilder changeDesc = new StringBuilder();
            if (totalDeleteCount > 0) {
                changeDesc.append("deleted_from_table:")
                          .append(table.tableId);
            }
            if (totalNewRecords > 0) {
                if (changeDesc.length() > 0) changeDesc.append(",");
                changeDesc.append("inserted_into_table:")
                          .append(table.tableId);
            }
            backend.insertSnapshotChanges(newSnap, changeDesc.toString(),
                    "ducklake-spark", "Spark merge");
        }

        // ---------------------------------------------------------------
        // Scanning and matching
        // ---------------------------------------------------------------

        private ScanResult scanAndMerge(
                String filePath, StructType targetSchema,
                List<Row> sourceRows, StructType sourceSchema,
                Map<String, List<Integer>> sourceIndex,
                Set<Long> existingDeletes,
                Map<String, String> logicalToPhysical,
                long[] columnIds, boolean[] sourceMatched) {

            List<Long> deletePositions = new ArrayList<>();
            List<Object[]> updatedRows = new ArrayList<>();

            StructField[] targetFields = targetSchema.fields();
            Comparable<?>[] mins = new Comparable<?>[targetFields.length];
            Comparable<?>[] maxs = new Comparable<?>[targetFields.length];
            long[] nullCounts = new long[targetFields.length];
            long[] valueCounts = new long[targetFields.length];

            try (ParquetFileReader reader = ParquetFileReader.open(
                    new Configuration(), new Path(filePath))) {
                MessageType fileSchema =
                        reader.getFooter().getFileMetaData().getSchema();

                long rowPosition = 0;
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    long rowCount = pages.getRowCount();
                    ColumnIOFactory factory = new ColumnIOFactory();
                    MessageColumnIO columnIO =
                            factory.getColumnIO(fileSchema);
                    RecordReader<Group> recordReader =
                            columnIO.getRecordReader(
                                    pages, new GroupRecordConverter(fileSchema));

                    for (long i = 0; i < rowCount; i++) {
                        Group group = recordReader.read();
                        long pos = rowPosition++;

                        if (existingDeletes.contains(pos)) {
                            continue;
                        }

                        String joinKey = extractTargetJoinKey(
                                group, fileSchema, logicalToPhysical,
                                targetSchema);

                        List<Integer> matchingSourceIndices =
                                sourceIndex.get(joinKey);

                        if (matchingSourceIndices != null
                                && !matchingSourceIndices.isEmpty()) {
                            // Cardinality check
                            if (matchingSourceIndices.size() > 1) {
                                throw new RuntimeException(
                                    "MERGE validation failed: multiple source "
                                    + "rows matched the same target row for "
                                    + "join key [" + joinKey + "]. MERGE "
                                    + "requires at most one source row per "
                                    + "target row.");
                            }

                            int srcIdx = matchingSourceIndices.get(0);
                            Row sourceRow = sourceRows.get(srcIdx);

                            for (MatchedClause clause :
                                    config.matchedClauses) {
                                if (clause.condition != null
                                        && !evaluateCondition(
                                            clause.condition,
                                            group, fileSchema,
                                            logicalToPhysical,
                                            targetSchema,
                                            sourceRow, sourceSchema)) {
                                    continue;
                                }

                                sourceMatched[srcIdx] = true;

                                switch (clause.action) {
                                    case DELETE:
                                        deletePositions.add(pos);
                                        break;

                                    case UPDATE_ALL:
                                        deletePositions.add(pos);
                                        Object[] uRow = buildUpdatedRowAll(
                                                group, fileSchema,
                                                logicalToPhysical,
                                                targetSchema,
                                                sourceRow, sourceSchema);
                                        for (int c = 0;
                                                c < targetFields.length;
                                                c++) {
                                            trackStats(uRow[c], c,
                                                    mins, maxs,
                                                    nullCounts, valueCounts);
                                        }
                                        updatedRows.add(uRow);
                                        break;

                                    case UPDATE:
                                        deletePositions.add(pos);
                                        Object[] pRow =
                                                buildUpdatedRowPartial(
                                                    group, fileSchema,
                                                    logicalToPhysical,
                                                    targetSchema,
                                                    sourceRow, sourceSchema,
                                                    clause.assignments);
                                        for (int c = 0;
                                                c < targetFields.length;
                                                c++) {
                                            trackStats(pRow[c], c,
                                                    mins, maxs,
                                                    nullCounts, valueCounts);
                                        }
                                        updatedRows.add(pRow);
                                        break;
                                }
                                break; // First matching clause wins
                            }
                        }
                    }
                }
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to scan file for merge: " + filePath, e);
            }

            List<DuckLakeWriterCommitMessage.ColumnStats> colStats =
                    new ArrayList<>();
            for (int c = 0; c < targetFields.length; c++) {
                String minStr = mins[c] != null ? mins[c].toString() : null;
                String maxStr = maxs[c] != null ? maxs[c].toString() : null;
                colStats.add(new DuckLakeWriterCommitMessage.ColumnStats(
                        columnIds[c], minStr, maxStr,
                        nullCounts[c], valueCounts[c]));
            }

            return new ScanResult(deletePositions, updatedRows, colStats);
        }

        // ---------------------------------------------------------------
        // Join key extraction and indexing
        // ---------------------------------------------------------------

        private Map<String, List<Integer>> buildSourceIndex(
                List<Row> sourceRows, StructType sourceSchema) {
            Map<String, List<Integer>> index = new HashMap<>();
            for (int i = 0; i < sourceRows.size(); i++) {
                String key = extractSourceJoinKey(
                        sourceRows.get(i), sourceSchema);
                index.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
            }
            return index;
        }

        private String extractSourceJoinKey(Row row, StructType schema) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < joinKeys.size(); i++) {
                if (i > 0) sb.append("|");
                String srcCol = joinKeys.get(i).sourceColumn;
                int fieldIdx = schema.fieldIndex(srcCol);
                Object val = row.isNullAt(fieldIdx)
                        ? "NULL" : row.get(fieldIdx);
                sb.append(val);
            }
            return sb.toString();
        }

        private String extractTargetJoinKey(
                Group group, MessageType fileSchema,
                Map<String, String> logicalToPhysical,
                StructType targetSchema) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < joinKeys.size(); i++) {
                if (i > 0) sb.append("|");
                String tgtCol = joinKeys.get(i).targetColumn;
                Object val = readGroupValue(group, fileSchema,
                        logicalToPhysical, tgtCol,
                        findDataType(targetSchema, tgtCol));
                sb.append(val == null ? "NULL" : val);
            }
            return sb.toString();
        }

        // ---------------------------------------------------------------
        // Row building
        // ---------------------------------------------------------------

        private Object[] buildUpdatedRowAll(
                Group group, MessageType fileSchema,
                Map<String, String> logicalToPhysical,
                StructType targetSchema,
                Row sourceRow, StructType sourceSchema) {
            StructField[] fields = targetSchema.fields();
            Object[] row = new Object[fields.length];

            for (int i = 0; i < fields.length; i++) {
                String colName = fields[i].name();
                int srcIdx = findFieldIndex(sourceSchema, colName);
                if (srcIdx >= 0 && !sourceRow.isNullAt(srcIdx)) {
                    row[i] = sourceRow.get(srcIdx);
                } else if (srcIdx >= 0) {
                    row[i] = null;
                } else {
                    row[i] = readGroupValue(group, fileSchema,
                            logicalToPhysical, colName,
                            fields[i].dataType());
                }
            }
            return row;
        }

        private Object[] buildUpdatedRowPartial(
                Group group, MessageType fileSchema,
                Map<String, String> logicalToPhysical,
                StructType targetSchema,
                Row sourceRow, StructType sourceSchema,
                Map<String, String> assignments) {
            StructField[] fields = targetSchema.fields();
            Object[] row = new Object[fields.length];

            for (int i = 0; i < fields.length; i++) {
                String colName = fields[i].name();
                if (assignments.containsKey(colName)) {
                    String srcCol = assignments.get(colName);
                    int srcIdx = sourceSchema.fieldIndex(srcCol);
                    row[i] = sourceRow.isNullAt(srcIdx)
                            ? null : sourceRow.get(srcIdx);
                } else {
                    row[i] = readGroupValue(group, fileSchema,
                            logicalToPhysical, colName,
                            fields[i].dataType());
                }
            }
            return row;
        }

        private List<Object[]> collectInsertRows(
                List<Row> sourceRows, StructType sourceSchema,
                boolean[] sourceMatched,
                StructType targetSchema,
                List<ColumnInfo> columns) {
            List<Object[]> insertRows = new ArrayList<>();

            for (NotMatchedClause clause : config.notMatchedClauses) {
                for (int i = 0; i < sourceRows.size(); i++) {
                    if (sourceMatched[i]) continue;

                    Row sourceRow = sourceRows.get(i);

                    if (clause.condition != null
                            && !evaluateSourceCondition(
                                clause.condition,
                                sourceRow, sourceSchema)) {
                        continue;
                    }

                    StructField[] targetFields = targetSchema.fields();
                    Object[] row = new Object[targetFields.length];

                    if (clause.action == InsertAction.INSERT_ALL) {
                        for (int c = 0; c < targetFields.length; c++) {
                            String colName = targetFields[c].name();
                            int srcIdx = findFieldIndex(
                                    sourceSchema, colName);
                            if (srcIdx >= 0
                                    && !sourceRow.isNullAt(srcIdx)) {
                                row[c] = sourceRow.get(srcIdx);
                            } else {
                                row[c] = null;
                            }
                        }
                    } else {
                        for (int c = 0; c < targetFields.length; c++) {
                            String colName = targetFields[c].name();
                            if (clause.assignments.containsKey(colName)) {
                                String srcCol =
                                        clause.assignments.get(colName);
                                int srcIdx =
                                        sourceSchema.fieldIndex(srcCol);
                                row[c] = sourceRow.isNullAt(srcIdx)
                                        ? null : sourceRow.get(srcIdx);
                            } else {
                                row[c] = null;
                            }
                        }
                    }

                    insertRows.add(row);
                    sourceMatched[i] = true;
                }
            }

            return insertRows;
        }

        // ---------------------------------------------------------------
        // Condition evaluation
        // ---------------------------------------------------------------

        private boolean evaluateCondition(
                String condition,
                Group targetGroup, MessageType fileSchema,
                Map<String, String> logicalToPhysical,
                StructType targetSchema,
                Row sourceRow, StructType sourceSchema) {
            String[] parts = condition.split("(?i)\\s+AND\\s+");
            for (String part : parts) {
                if (!evaluateSingleCondition(part.trim(),
                        targetGroup, fileSchema, logicalToPhysical,
                        targetSchema, sourceRow, sourceSchema)) {
                    return false;
                }
            }
            return true;
        }

        private boolean evaluateSingleCondition(
                String expr,
                Group targetGroup, MessageType fileSchema,
                Map<String, String> logicalToPhysical,
                StructType targetSchema,
                Row sourceRow, StructType sourceSchema) {
            String[] ops = {"!=", ">=", "<=", ">", "<", "="};
            for (String op : ops) {
                int idx = findOperator(expr, op);
                if (idx >= 0) {
                    String lhs = expr.substring(0, idx).trim();
                    String rhs = expr.substring(
                            idx + op.length()).trim();

                    Object lhsVal = resolveValue(lhs,
                            targetGroup, fileSchema,
                            logicalToPhysical, targetSchema,
                            sourceRow, sourceSchema);
                    Object rhsVal = resolveValue(rhs,
                            targetGroup, fileSchema,
                            logicalToPhysical, targetSchema,
                            sourceRow, sourceSchema);

                    int cmp = DuckLakeRowFilterEvaluator
                            .compareValues(lhsVal, rhsVal);

                    switch (op) {
                        case "=":  return cmp == 0;
                        case "!=": return cmp != 0;
                        case ">":  return cmp > 0;
                        case ">=": return cmp >= 0;
                        case "<":  return cmp < 0;
                        case "<=": return cmp <= 0;
                    }
                }
            }
            return true;
        }

        /**
         * Find operator position, avoiding confusion between
         * >, >=, <, <=, =, !=.
         */
        private int findOperator(String expr, String op) {
            int idx = expr.indexOf(op);
            if (idx < 0) return -1;

            // For single-char ops, make sure they're not part of
            // a multi-char op
            if (op.equals("=")) {
                // Make sure it's not != or >= or <=
                if (idx > 0) {
                    char before = expr.charAt(idx - 1);
                    if (before == '!' || before == '>'
                            || before == '<') {
                        return -1;
                    }
                }
            } else if (op.equals(">")) {
                // Make sure it's not >=
                if (idx + 1 < expr.length()
                        && expr.charAt(idx + 1) == '=') {
                    return -1;
                }
            } else if (op.equals("<")) {
                // Make sure it's not <=
                if (idx + 1 < expr.length()
                        && expr.charAt(idx + 1) == '=') {
                    return -1;
                }
            }
            return idx;
        }

        private Object resolveValue(
                String token,
                Group targetGroup, MessageType fileSchema,
                Map<String, String> logicalToPhysical,
                StructType targetSchema,
                Row sourceRow, StructType sourceSchema) {
            // Number literal
            try {
                if (token.contains(".")) {
                    return Double.parseDouble(token);
                }
                return Long.parseLong(token);
            } catch (NumberFormatException ignored) {}

            // Quoted string literal
            if ((token.startsWith("'") && token.endsWith("'"))
                    || (token.startsWith("\"")
                        && token.endsWith("\""))) {
                return token.substring(1, token.length() - 1);
            }

            // Column reference with alias
            if (token.contains(".")) {
                String[] parts = token.split("\\.", 2);
                String alias = parts[0].trim();
                String col = parts[1].trim();

                if (alias.equalsIgnoreCase(config.sourceAlias)) {
                    int srcIdx = findFieldIndex(sourceSchema, col);
                    if (srcIdx >= 0) {
                        return sourceRow.isNullAt(srcIdx)
                                ? null : sourceRow.get(srcIdx);
                    }
                } else {
                    return readGroupValue(targetGroup, fileSchema,
                            logicalToPhysical, col,
                            findDataType(targetSchema, col));
                }
            }

            // Unqualified: try source first, then target
            int srcIdx = findFieldIndex(sourceSchema, token);
            if (srcIdx >= 0) {
                return sourceRow.isNullAt(srcIdx)
                        ? null : sourceRow.get(srcIdx);
            }
            return readGroupValue(targetGroup, fileSchema,
                    logicalToPhysical, token,
                    findDataType(targetSchema, token));
        }

        private boolean evaluateSourceCondition(
                String condition, Row sourceRow,
                StructType sourceSchema) {
            String[] parts = condition.split("(?i)\\s+AND\\s+");
            for (String part : parts) {
                if (!evaluateSingleSourceCondition(
                        part.trim(), sourceRow, sourceSchema)) {
                    return false;
                }
            }
            return true;
        }

        private boolean evaluateSingleSourceCondition(
                String expr, Row sourceRow,
                StructType sourceSchema) {
            String[] ops = {"!=", ">=", "<=", ">", "<", "="};
            for (String op : ops) {
                int idx = findOperator(expr, op);
                if (idx >= 0) {
                    String lhs = expr.substring(0, idx).trim();
                    String rhs = expr.substring(
                            idx + op.length()).trim();

                    Object lhsVal = resolveSourceValue(
                            lhs, sourceRow, sourceSchema);
                    Object rhsVal = resolveSourceValue(
                            rhs, sourceRow, sourceSchema);

                    int cmp = DuckLakeRowFilterEvaluator
                            .compareValues(lhsVal, rhsVal);

                    switch (op) {
                        case "=":  return cmp == 0;
                        case "!=": return cmp != 0;
                        case ">":  return cmp > 0;
                        case ">=": return cmp >= 0;
                        case "<":  return cmp < 0;
                        case "<=": return cmp <= 0;
                    }
                }
            }
            return true;
        }

        private Object resolveSourceValue(
                String token, Row sourceRow,
                StructType sourceSchema) {
            try {
                if (token.contains(".")) {
                    return Double.parseDouble(token);
                }
                return Long.parseLong(token);
            } catch (NumberFormatException ignored) {}

            if ((token.startsWith("'") && token.endsWith("'"))
                    || (token.startsWith("\"")
                        && token.endsWith("\""))) {
                return token.substring(1, token.length() - 1);
            }

            String col = token.contains(".")
                    ? token.split("\\.", 2)[1].trim() : token;
            int srcIdx = findFieldIndex(sourceSchema, col);
            if (srcIdx >= 0) {
                return sourceRow.isNullAt(srcIdx)
                        ? null : sourceRow.get(srcIdx);
            }
            return null;
        }

        // ---------------------------------------------------------------
        // Parquet reading helpers
        // ---------------------------------------------------------------

        private Object readGroupValue(
                Group group, MessageType fileSchema,
                Map<String, String> logicalToPhysical,
                String logicalName, DataType sparkType) {
            if (sparkType == null) return null;
            String physicalName = logicalToPhysical.getOrDefault(
                    logicalName, logicalName);
            int fieldIndex;
            try {
                fieldIndex = fileSchema.getFieldIndex(physicalName);
            } catch (Exception e) {
                return null;
            }
            if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                return null;
            }
            try {
                if (sparkType instanceof BooleanType) {
                    return group.getBoolean(fieldIndex, 0);
                } else if (sparkType instanceof ByteType) {
                    return (byte) group.getInteger(fieldIndex, 0);
                } else if (sparkType instanceof ShortType) {
                    return (short) group.getInteger(fieldIndex, 0);
                } else if (sparkType instanceof IntegerType) {
                    return group.getInteger(fieldIndex, 0);
                } else if (sparkType instanceof LongType) {
                    return group.getLong(fieldIndex, 0);
                } else if (sparkType instanceof FloatType) {
                    return group.getFloat(fieldIndex, 0);
                } else if (sparkType instanceof DoubleType) {
                    return group.getDouble(fieldIndex, 0);
                } else if (sparkType instanceof StringType) {
                    return group.getString(fieldIndex, 0);
                } else {
                    return group.getValueToString(fieldIndex, 0);
                }
            } catch (Exception e) {
                return null;
            }
        }

        // ---------------------------------------------------------------
        // File writing
        // ---------------------------------------------------------------

        private void writeDataRows(String absolutePath,
                                    StructType sparkSchema,
                                    long[] columnIds,
                                    List<Object[]> rows) {
            writeDataRowsWithStats(
                    absolutePath, sparkSchema, columnIds, rows);
        }

        private List<DuckLakeWriterCommitMessage.ColumnStats>
                writeDataRowsWithStats(
                    String absolutePath, StructType sparkSchema,
                    long[] columnIds, List<Object[]> rows) {
            MessageType parquetSchema =
                    buildParquetSchema(sparkSchema, columnIds);
            SimpleGroupFactory groupFactory =
                    new SimpleGroupFactory(parquetSchema);

            StructField[] fields = sparkSchema.fields();
            Comparable<?>[] mins =
                    new Comparable<?>[fields.length];
            Comparable<?>[] maxs =
                    new Comparable<?>[fields.length];
            long[] nullCounts = new long[fields.length];
            long[] valueCnts = new long[fields.length];

            File parentDir = new File(absolutePath).getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }

            try (ParquetWriter<Group> writer =
                    ExampleParquetWriter
                            .builder(new Path(absolutePath))
                            .withType(parquetSchema)
                            .withConf(new Configuration())
                            .withWriteMode(
                                    ParquetFileWriter.Mode.CREATE)
                            .build()) {
                for (Object[] row : rows) {
                    Group group = groupFactory.newGroup();
                    for (int i = 0; i < fields.length; i++) {
                        if (row[i] != null) {
                            writeField(group, i, row[i],
                                    fields[i].dataType());
                        }
                        trackStats(row[i], i,
                                mins, maxs,
                                nullCounts, valueCnts);
                    }
                    writer.write(group);
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to write data file: "
                                + absolutePath, e);
            }

            List<DuckLakeWriterCommitMessage.ColumnStats> colStats =
                    new ArrayList<>();
            for (int i = 0; i < fields.length; i++) {
                String minStr = mins[i] != null
                        ? mins[i].toString() : null;
                String maxStr = maxs[i] != null
                        ? maxs[i].toString() : null;
                colStats.add(
                        new DuckLakeWriterCommitMessage.ColumnStats(
                                columnIds[i], minStr, maxStr,
                                nullCounts[i], valueCnts[i]));
            }
            return colStats;
        }

        private void writeField(Group group, int fieldIndex,
                                 Object value, DataType type) {
            if (type instanceof BooleanType) {
                group.add(fieldIndex, (Boolean) value);
            } else if (type instanceof ByteType) {
                group.add(fieldIndex, (int) ((Byte) value));
            } else if (type instanceof ShortType) {
                group.add(fieldIndex, (int) ((Short) value));
            } else if (type instanceof IntegerType) {
                group.add(fieldIndex, (Integer) value);
            } else if (type instanceof LongType) {
                group.add(fieldIndex, (Long) value);
            } else if (type instanceof FloatType) {
                group.add(fieldIndex, (Float) value);
            } else if (type instanceof DoubleType) {
                group.add(fieldIndex, (Double) value);
            } else if (type instanceof StringType) {
                group.add(fieldIndex, value.toString());
            } else {
                group.add(fieldIndex, value.toString());
            }
        }

        // ---------------------------------------------------------------
        // Stats tracking
        // ---------------------------------------------------------------

        @SuppressWarnings({"unchecked", "rawtypes"})
        private void trackStats(Object value, int colIdx,
                                 Comparable<?>[] mins,
                                 Comparable<?>[] maxs,
                                 long[] nullCounts,
                                 long[] valueCounts) {
            if (value == null) {
                nullCounts[colIdx]++;
                return;
            }
            valueCounts[colIdx]++;
            if (value instanceof Comparable) {
                Comparable comp = (Comparable) value;
                if (mins[colIdx] == null
                        || comp.compareTo(mins[colIdx]) < 0) {
                    mins[colIdx] = comp;
                }
                if (maxs[colIdx] == null
                        || comp.compareTo(maxs[colIdx]) > 0) {
                    maxs[colIdx] = comp;
                }
            }
        }

        // ---------------------------------------------------------------
        // Schema helpers
        // ---------------------------------------------------------------

        private static MessageType buildParquetSchema(
                StructType sparkSchema, long[] columnIds) {
            Types.MessageTypeBuilder builder =
                    Types.buildMessage();
            for (int i = 0; i < sparkSchema.fields().length; i++) {
                StructField field = sparkSchema.fields()[i];
                int fieldId = (int) columnIds[i];
                builder.addField(sparkTypeToParquetType(
                        field.name(), field.dataType(),
                        field.nullable(), fieldId));
            }
            return builder.named("spark_schema");
        }

        private static org.apache.parquet.schema.Type
                sparkTypeToParquetType(
                    String name, DataType type,
                    boolean nullable, int fieldId) {
            PrimitiveType.PrimitiveTypeName typeName;
            LogicalTypeAnnotation logicalType = null;

            if (type instanceof BooleanType) {
                typeName = PrimitiveType.PrimitiveTypeName.BOOLEAN;
            } else if (type instanceof ByteType
                    || type instanceof ShortType
                    || type instanceof IntegerType) {
                typeName = PrimitiveType.PrimitiveTypeName.INT32;
            } else if (type instanceof LongType) {
                typeName = PrimitiveType.PrimitiveTypeName.INT64;
            } else if (type instanceof FloatType) {
                typeName = PrimitiveType.PrimitiveTypeName.FLOAT;
            } else if (type instanceof DoubleType) {
                typeName = PrimitiveType.PrimitiveTypeName.DOUBLE;
            } else {
                typeName = PrimitiveType.PrimitiveTypeName.BINARY;
                logicalType = LogicalTypeAnnotation.stringType();
            }

            if (logicalType != null) {
                return nullable
                    ? Types.optional(typeName).as(logicalType)
                            .id(fieldId).named(name)
                    : Types.required(typeName).as(logicalType)
                            .id(fieldId).named(name);
            } else {
                return nullable
                    ? Types.optional(typeName)
                            .id(fieldId).named(name)
                    : Types.required(typeName)
                            .id(fieldId).named(name);
            }
        }

        // ---------------------------------------------------------------
        // Common helpers
        // ---------------------------------------------------------------

        private Set<Long> loadExistingDeletes(
                DuckLakeMetadataBackend backend, long tableId,
                long dataFileId, long snapshotId,
                String dataPath) throws SQLException {
            Set<Long> deleted = new HashSet<>();
            List<DeleteFileInfo> deleteFiles =
                    backend.getDeleteFiles(
                            tableId, dataFileId, snapshotId);
            for (DeleteFileInfo df : deleteFiles) {
                String path = df.pathIsRelative
                        ? dataPath + df.path : df.path;
                try (ParquetFileReader reader =
                        ParquetFileReader.open(
                                new Configuration(),
                                new Path(path))) {
                    MessageType schema = reader.getFooter()
                            .getFileMetaData().getSchema();
                    PageReadStore pages;
                    while ((pages = reader.readNextRowGroup())
                            != null) {
                        ColumnIOFactory factory =
                                new ColumnIOFactory();
                        MessageColumnIO columnIO =
                                factory.getColumnIO(schema);
                        RecordReader<Group> recordReader =
                                columnIO.getRecordReader(pages,
                                        new GroupRecordConverter(
                                                schema));
                        for (long i = 0;
                                i < pages.getRowCount(); i++) {
                            Group group = recordReader.read();
                            int fieldIdx =
                                    schema.getFieldIndex("row_id");
                            deleted.add(
                                    group.getLong(fieldIdx, 0));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to read delete file: "
                                    + path, e);
                }
            }
            return deleted;
        }

        private Map<String, String> buildLogicalToPhysical(
                DuckLakeMetadataBackend backend,
                long mappingId,
                Map<Long, String> colIdToName)
                throws SQLException {
            Map<String, String> logicalToPhysical =
                    new HashMap<>();
            if (mappingId >= 0) {
                Map<Long, String> nameMapping =
                        backend.getNameMapping(mappingId);
                for (Map.Entry<Long, String> entry :
                        nameMapping.entrySet()) {
                    long fieldId = entry.getKey();
                    String physicalName = entry.getValue();
                    String logicalName =
                            colIdToName.get(fieldId);
                    if (logicalName != null
                            && !physicalName.equals(
                                    logicalName)) {
                        logicalToPhysical.put(
                                logicalName, physicalName);
                    }
                }
            }
            return logicalToPhysical;
        }

        private static int findFieldIndex(
                StructType schema, String name) {
            try {
                return schema.fieldIndex(name);
            } catch (Exception e) {
                return -1;
            }
        }

        private static DataType findDataType(
                StructType schema, String name) {
            int idx = findFieldIndex(schema, name);
            return idx >= 0
                    ? schema.fields()[idx].dataType() : null;
        }
    }

    // =================================================================
    // ON condition parser
    // =================================================================

    static List<JoinKeyPair> parseOnCondition(
            String condition, String sourceAlias) {
        List<JoinKeyPair> keys = new ArrayList<>();

        String[] parts = condition.split("(?i)\\s+AND\\s+");
        for (String part : parts) {
            part = part.trim();
            int eqIdx = part.indexOf('=');
            if (eqIdx < 0) {
                throw new IllegalArgumentException(
                        "Invalid ON condition: expected equality "
                        + "expression, got: " + part);
            }

            if (eqIdx > 0 && part.charAt(eqIdx - 1) == '!') {
                throw new IllegalArgumentException(
                        "Invalid ON condition: != not supported "
                        + "in join condition, got: " + part);
            }

            String lhs = part.substring(0, eqIdx).trim();
            String rhs = part.substring(eqIdx + 1).trim();

            String[] lhsParts = lhs.split("\\.", 2);
            String[] rhsParts = rhs.split("\\.", 2);

            String lhsAlias = lhsParts.length == 2
                    ? lhsParts[0].trim() : null;
            String lhsCol = lhsParts.length == 2
                    ? lhsParts[1].trim() : lhsParts[0].trim();
            String rhsAlias = rhsParts.length == 2
                    ? rhsParts[0].trim() : null;
            String rhsCol = rhsParts.length == 2
                    ? rhsParts[1].trim() : rhsParts[0].trim();

            String targetCol, sourceCol;

            if (lhsAlias != null
                    && lhsAlias.equalsIgnoreCase(sourceAlias)) {
                sourceCol = lhsCol;
                targetCol = rhsCol;
            } else if (rhsAlias != null
                    && rhsAlias.equalsIgnoreCase(sourceAlias)) {
                sourceCol = rhsCol;
                targetCol = lhsCol;
            } else {
                targetCol = lhsCol;
                sourceCol = rhsCol;
            }

            keys.add(new JoinKeyPair(targetCol, sourceCol));
        }

        if (keys.isEmpty()) {
            throw new IllegalArgumentException(
                    "ON condition produced no join keys: "
                            + condition);
        }

        return keys;
    }

    // =================================================================
    // Result data classes
    // =================================================================

    private static class DeleteFileResult {
        final long dataFileId;
        final String relativePath;
        final long deleteCount;
        final long fileSize;

        DeleteFileResult(long dataFileId, String relativePath,
                         long deleteCount, long fileSize) {
            this.dataFileId = dataFileId;
            this.relativePath = relativePath;
            this.deleteCount = deleteCount;
            this.fileSize = fileSize;
        }
    }

    private static class NewDataFileResult {
        final String relativePath;
        final long recordCount;
        final long fileSize;
        final List<DuckLakeWriterCommitMessage.ColumnStats>
                columnStats;

        NewDataFileResult(
                String relativePath, long recordCount,
                long fileSize,
                List<DuckLakeWriterCommitMessage.ColumnStats>
                        columnStats) {
            this.relativePath = relativePath;
            this.recordCount = recordCount;
            this.fileSize = fileSize;
            this.columnStats = columnStats;
        }
    }

    private static class ScanResult {
        final List<Long> deletePositions;
        final List<Object[]> updatedRows;
        final List<DuckLakeWriterCommitMessage.ColumnStats>
                updateColumnStats;

        ScanResult(List<Long> deletePositions,
                   List<Object[]> updatedRows,
                   List<DuckLakeWriterCommitMessage.ColumnStats>
                           updateColumnStats) {
            this.deletePositions = deletePositions;
            this.updatedRows = updatedRows;
            this.updateColumnStats = updateColumnStats;
        }
    }
}
