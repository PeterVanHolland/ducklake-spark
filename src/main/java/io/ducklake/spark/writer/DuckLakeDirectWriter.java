package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeCatalog;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

/**
 * Fast-path writer that bypasses Spark's distributed execution.
 * Collects DataFrame to driver, writes Parquet directly, commits to SQLite.
 * ~10x faster than the V2 DataSource write path for small batches.
 */
public class DuckLakeDirectWriter {

    /**
     * Write a DataFrame directly to a DuckLake table without going through
     * Spark's distributed write pipeline.
     */
    public static void write(String catalogPath, String tableName, Dataset<Row> data) {
        write(catalogPath, tableName, data, null);
    }

    public static void write(String catalogPath, String tableName, Dataset<Row> data, String dataPathOverride) {
        StructType schema = data.schema();
        List<Row> rows = data.collectAsList();

        if (rows.isEmpty()) return;

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPathOverride)) {
            backend.beginTransaction();
            try {
                long currentSnap = backend.getCurrentSnapshotId();
                CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);

                // Resolve table
                String dataPath = backend.getDataPath();
                TableInfo tableInfo = backend.getTable(tableName, "main", currentSnap);
                if (tableInfo == null) throw new RuntimeException("Table not found: " + tableName);

                List<ColumnInfo> columns = backend.getColumns(tableInfo.tableId, currentSnap);
                long[] columnIds = columns.stream().mapToLong(c -> c.columnId).toArray();

                // Write Parquet file
                String tablePath = tableInfo.path != null ? tableInfo.path : "main/" + tableName + "/";
                String fileName = "ducklake-" + UUID.randomUUID() + ".parquet";
                String relativePath = tablePath + fileName;
                String absolutePath = dataPath + relativePath;

                File parentDir = new File(absolutePath).getParentFile();
                if (parentDir != null && !parentDir.exists()) parentDir.mkdirs();

                MessageType parquetSchema = buildParquetSchema(schema, columnIds);
                long recordCount = 0;
                ColumnStatsAccumulator[] accums = new ColumnStatsAccumulator[schema.fields().length];
                for (int i = 0; i < accums.length; i++) {
                    accums[i] = new ColumnStatsAccumulator(schema.fields()[i].dataType());
                }

                Configuration conf = new Configuration();
                try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(absolutePath))
                        .withType(parquetSchema).withConf(conf)
                        .withWriteMode(ParquetFileWriter.Mode.CREATE).build()) {

                    SimpleGroupFactory factory = new SimpleGroupFactory(parquetSchema);
                    for (Row row : rows) {
                        Group group = factory.newGroup();
                        for (int i = 0; i < schema.fields().length; i++) {
                            if (row.isNullAt(i)) {
                                accums[i].addNull();
                                continue;
                            }
                            Object val = writeField(group, i, row, i, schema.fields()[i].dataType());
                            accums[i].addValue(val);
                        }
                        writer.write(group);
                        recordCount++;
                    }
                }

                long fileSize = new File(absolutePath).length();

                // Commit metadata
                long newSnap = currentSnap + 1;
                long fileId = snapInfo.nextFileId;
                backend.createSnapshotAtomically(currentSnap, newSnap, snapInfo.schemaVersion,
                        snapInfo.nextCatalogId, fileId + 1);

                TableStats tableStats = backend.getTableStats(tableInfo.tableId);
                long rowIdStart = tableStats.nextRowId;

                backend.insertDataFile(fileId, tableInfo.tableId, newSnap, 0,
                        relativePath, recordCount, fileSize, rowIdStart);

                // Batch column stats
                List<long[]> statsData = new ArrayList<>();
                List<String[]> statsStrings = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    statsData.add(new long[]{columnIds[i], accums[i].valueCount, accums[i].nullCount});
                    statsStrings.add(new String[]{accums[i].getMinString(), accums[i].getMaxString()});
                }
                backend.insertColumnStatsBatch(fileId, tableInfo.tableId, statsData, statsStrings);

                backend.updateTableStats(tableInfo.tableId,
                        tableStats.recordCount + recordCount,
                        rowIdStart + recordCount,
                        tableStats.fileSizeBytes + fileSize);

                backend.insertSnapshotChanges(newSnap, "inserted_into_table:" + tableInfo.tableId,
                        "ducklake-spark", "Direct write");

                backend.commitTransaction();
            } catch (Exception e) {
                try { backend.rollbackTransaction(); } catch (SQLException re) { e.addSuppressed(re); }
                throw new RuntimeException("Direct write failed", e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Direct write failed", e);
        }
    }

    // ---------------------------------------------------------------
    // Parquet helpers (same logic as DuckLakeDataWriter)
    // ---------------------------------------------------------------

    private static MessageType buildParquetSchema(StructType sparkSchema, long[] columnIds) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < sparkSchema.fields().length; i++) {
            StructField field = sparkSchema.fields()[i];
            int fieldId = (int) columnIds[i];
            builder.addField(sparkTypeToParquetType(field.name(), field.dataType(), field.nullable(), fieldId));
        }
        return builder.named("spark_schema");
    }

    private static org.apache.parquet.schema.Type sparkTypeToParquetType(
            String name, DataType type, boolean nullable, int fieldId) {
        if (type instanceof BooleanType) return prim(nullable, PrimitiveTypeName.BOOLEAN, null, fieldId, name);
        if (type instanceof IntegerType) return prim(nullable, PrimitiveTypeName.INT32, null, fieldId, name);
        if (type instanceof LongType) return prim(nullable, PrimitiveTypeName.INT64, null, fieldId, name);
        if (type instanceof FloatType) return prim(nullable, PrimitiveTypeName.FLOAT, null, fieldId, name);
        if (type instanceof DoubleType) return prim(nullable, PrimitiveTypeName.DOUBLE, null, fieldId, name);
        if (type instanceof StringType) return prim(nullable, PrimitiveTypeName.BINARY,
                LogicalTypeAnnotation.stringType(), fieldId, name);
        if (type instanceof DateType) return prim(nullable, PrimitiveTypeName.INT32,
                LogicalTypeAnnotation.dateType(), fieldId, name);
        if (type instanceof TimestampType) return prim(nullable, PrimitiveTypeName.INT64,
                LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS), fieldId, name);
        return prim(nullable, PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType(), fieldId, name);
    }

    private static PrimitiveType prim(boolean nullable, PrimitiveTypeName t,
                                       LogicalTypeAnnotation a, int id, String name) {
        if (a != null) return nullable ? Types.optional(t).as(a).id(id).named(name) : Types.required(t).as(a).id(id).named(name);
        return nullable ? Types.optional(t).id(id).named(name) : Types.required(t).id(id).named(name);
    }

    private static Object writeField(Group group, int fi, Row row, int ord, DataType type) {
        if (type instanceof BooleanType) { boolean v = row.getBoolean(ord); group.add(fi, v); return v; }
        if (type instanceof IntegerType) { int v = row.getInt(ord); group.add(fi, v); return v; }
        if (type instanceof LongType) { long v = row.getLong(ord); group.add(fi, v); return v; }
        if (type instanceof FloatType) { float v = row.getFloat(ord); group.add(fi, v); return v; }
        if (type instanceof DoubleType) { double v = row.getDouble(ord); group.add(fi, v); return v; }
        if (type instanceof StringType) { String v = row.getString(ord); group.add(fi, v); return v; }
        String v = row.get(ord).toString(); group.add(fi, v); return v;
    }

    /**
     * Write raw Row data directly — no Spark DataFrame creation or collect needed.
     * This is the fastest path: Java rows -> Parquet -> SQLite metadata.
     */
    public static void writeRows(String catalogPath, String tableName,
                                  List<Row> rows, StructType schema) {
        writeRows(catalogPath, tableName, rows, schema, null);
    }

    public static void writeRows(String catalogPath, String tableName,
                                  List<Row> rows, StructType schema, String dataPathOverride) {
        if (rows.isEmpty()) return;

        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPathOverride)) {
            backend.beginTransaction();
            try {
                long currentSnap = backend.getCurrentSnapshotId();
                CatalogState snapInfo = backend.getSnapshotInfo(currentSnap);

                String dataPath = backend.getDataPath();
                TableInfo tableInfo = backend.getTable(tableName, "main", currentSnap);
                if (tableInfo == null) throw new RuntimeException("Table not found: " + tableName);

                List<ColumnInfo> columns = backend.getColumns(tableInfo.tableId, currentSnap);
                long[] columnIds = columns.stream().mapToLong(c -> c.columnId).toArray();

                String tablePath = tableInfo.path != null ? tableInfo.path : "main/" + tableName + "/";
                String fileName = "ducklake-" + UUID.randomUUID() + ".parquet";
                String relativePath = tablePath + fileName;
                String absolutePath = dataPath + relativePath;

                File parentDir = new File(absolutePath).getParentFile();
                if (parentDir != null && !parentDir.exists()) parentDir.mkdirs();

                MessageType parquetSchema = buildParquetSchema(schema, columnIds);
                long recordCount = 0;
                ColumnStatsAccumulator[] accums = new ColumnStatsAccumulator[schema.fields().length];
                for (int i = 0; i < accums.length; i++) {
                    accums[i] = new ColumnStatsAccumulator(schema.fields()[i].dataType());
                }

                Configuration conf = new Configuration();
                try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(absolutePath))
                        .withType(parquetSchema).withConf(conf)
                        .withWriteMode(ParquetFileWriter.Mode.CREATE).build()) {

                    SimpleGroupFactory factory = new SimpleGroupFactory(parquetSchema);
                    for (Row row : rows) {
                        Group group = factory.newGroup();
                        for (int i = 0; i < schema.fields().length; i++) {
                            if (row.isNullAt(i)) { accums[i].addNull(); continue; }
                            Object val = writeField(group, i, row, i, schema.fields()[i].dataType());
                            accums[i].addValue(val);
                        }
                        writer.write(group);
                        recordCount++;
                    }
                }

                long fileSize = new File(absolutePath).length();

                long newSnap = currentSnap + 1;
                long fileId = snapInfo.nextFileId;
                backend.createSnapshotAtomically(currentSnap, newSnap, snapInfo.schemaVersion,
                        snapInfo.nextCatalogId, fileId + 1);

                TableStats tableStats = backend.getTableStats(tableInfo.tableId);
                long rowIdStart = tableStats.nextRowId;

                backend.insertDataFile(fileId, tableInfo.tableId, newSnap, 0,
                        relativePath, recordCount, fileSize, rowIdStart);

                List<long[]> statsData = new ArrayList<>();
                List<String[]> statsStrings = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    statsData.add(new long[]{columnIds[i], accums[i].valueCount, accums[i].nullCount});
                    statsStrings.add(new String[]{accums[i].getMinString(), accums[i].getMaxString()});
                }
                backend.insertColumnStatsBatch(fileId, tableInfo.tableId, statsData, statsStrings);

                backend.updateTableStats(tableInfo.tableId,
                        tableStats.recordCount + recordCount,
                        rowIdStart + recordCount,
                        tableStats.fileSizeBytes + fileSize);

                backend.insertSnapshotChanges(newSnap, "inserted_into_table:" + tableInfo.tableId,
                        "ducklake-spark", "Direct write");

                backend.commitTransaction();
            } catch (Exception e) {
                try { backend.rollbackTransaction(); } catch (SQLException re) { e.addSuppressed(re); }
                throw new RuntimeException("Direct write failed", e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Direct write failed", e);
        }
    }

    // Inline stats accumulator (same as DuckLakeDataWriter)
    private static class ColumnStatsAccumulator {
        final DataType dataType;
        long nullCount = 0, valueCount = 0;
        Comparable<?> minValue = null, maxValue = null;
        ColumnStatsAccumulator(DataType dt) { this.dataType = dt; }
        void addNull() { nullCount++; }
        @SuppressWarnings({"unchecked", "rawtypes"})
        void addValue(Object value) {
            if (value == null) { valueCount++; return; }
            valueCount++;
            Comparable comp = (Comparable) value;
            if (minValue == null || comp.compareTo(minValue) < 0) minValue = comp;
            if (maxValue == null || comp.compareTo(maxValue) > 0) maxValue = comp;
        }
        String getMinString() { return minValue != null ? minValue.toString() : null; }
        String getMaxString() { return maxValue != null ? maxValue.toString() : null; }
    }
}
