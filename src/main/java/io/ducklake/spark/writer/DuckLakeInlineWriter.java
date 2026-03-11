package io.ducklake.spark.writer;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Handles data inlining for small writes: stores rows directly in the catalog
 * database (SQLite or PostgreSQL) instead of creating Parquet files.
 *
 * This avoids the overhead of creating small Parquet files for tiny inserts.
 * The inlined data is stored in dynamically-created tables within the catalog
 * database, keyed by table_id and schema_version.
 *
 * The ducklake_inlined_data_tables registry tracks which inlined tables exist.
 * Each inlined table has columns: row_id, begin_snapshot, end_snapshot, plus
 * one column per table column (mapped to SQLite/PostgreSQL-compatible types).
 */
public class DuckLakeInlineWriter {

    private final DuckLakeMetadataBackend backend;

    public DuckLakeInlineWriter(DuckLakeMetadataBackend backend) {
        this.backend = backend;
    }

    /**
     * Check if a write should be inlined based on the configured row limit
     * and current inlined row count.
     */
    public boolean shouldInline(long tableId, long snapshotId, long newRowCount, int inlineRowLimit) {
        if (inlineRowLimit <= 0) {
            return false;
        }
        long currentCount = getInlinedActiveRowCount(tableId, snapshotId);
        return (currentCount + newRowCount) <= inlineRowLimit;
    }

    /**
     * Count active (non-ended) inlined rows across all inlined data tables for a table.
     */
    public long getInlinedActiveRowCount(long tableId, long snapshotId) {
        try {
            Connection conn = backend.getConnectionForInlining();
            List<String> tableNames = getInlinedTableNames(conn, tableId);
            long total = 0;
            for (String tblName : tableNames) {
                String safeName = tblName.replace("\"", "\"\"");
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT COUNT(*) FROM \"" + safeName + "\" " +
                        "WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                    ps.setLong(1, snapshotId);
                    ps.setLong(2, snapshotId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            total += rs.getLong(1);
                        }
                    }
                } catch (SQLException e) {
                    // Table may not exist yet, skip
                }
            }
            return total;
        } catch (SQLException e) {
            return 0;
        }
    }

    /**
     * Insert rows into the inlined data table. Creates the table if necessary.
     * Returns the number of rows inserted.
     */
    public int insertInlinedRows(List<InternalRow> rows, StructType schema,
                                  long tableId, long schemaVersion,
                                  List<ColumnInfo> columns, long snapshotId,
                                  long rowIdStart) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        String tblName = ensureInlinedTable(conn, tableId, schemaVersion, columns);
        String safeName = tblName.replace("\"", "\"\"");

        // Build column names list
        List<String> colNames = new ArrayList<>();
        colNames.add("row_id");
        colNames.add("begin_snapshot");
        colNames.add("end_snapshot");
        for (ColumnInfo col : columns) {
            colNames.add("\"" + col.name.replace("\"", "\"\"") + "\"");
        }

        String placeholders = String.join(", ", Collections.nCopies(colNames.size(), "?"));
        String insertSql = "INSERT INTO \"" + safeName + "\" (" +
                String.join(", ", colNames) + ") VALUES (" + placeholders + ")";

        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (int r = 0; r < rows.size(); r++) {
                InternalRow row = rows.get(r);
                ps.setLong(1, rowIdStart + r);
                ps.setLong(2, snapshotId);
                ps.setNull(3, Types.BIGINT); // end_snapshot = NULL (active)

                for (int c = 0; c < columns.size(); c++) {
                    ColumnInfo col = columns.get(c);
                    int paramIdx = c + 4; // 1-based, after row_id, begin_snapshot, end_snapshot

                    int fieldIdx = findFieldIndex(schema, col.name);
                    if (fieldIdx < 0 || row.isNullAt(fieldIdx)) {
                        ps.setNull(paramIdx, Types.VARCHAR);
                    } else {
                        Object value = extractValue(row, fieldIdx, schema.fields()[fieldIdx].dataType());
                        String serialized = serializeValue(value, col.type);
                        if (serialized == null) {
                            ps.setNull(paramIdx, Types.VARCHAR);
                        } else {
                            ps.setString(paramIdx, serialized);
                        }
                    }
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return rows.size();
    }

    /**
     * End (soft-delete) all active inlined rows for a table at a given snapshot.
     * Used for overwrite mode.
     */
    public void endAllInlinedRows(long tableId, long snapshotId) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        List<String> tableNames = getInlinedTableNames(conn, tableId);
        for (String tblName : tableNames) {
            String safeName = tblName.replace("\"", "\"\"");
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE \"" + safeName + "\" SET end_snapshot = ? " +
                    "WHERE end_snapshot IS NULL")) {
                ps.setLong(1, snapshotId);
                ps.executeUpdate();
            } catch (SQLException e) {
                // Table may not exist, skip
            }
        }
    }

    /**
     * Read active inlined rows for a table at a given snapshot.
     * Returns a list of maps where each map is column_name -> string_value.
     */
    public List<Map<String, String>> readInlinedRows(long tableId, long snapshotId) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        List<String> tableNames = getInlinedTableNames(conn, tableId);
        List<Map<String, String>> allRows = new ArrayList<>();

        for (String tblName : tableNames) {
            String safeName = tblName.replace("\"", "\"\"");
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT * FROM \"" + safeName + "\" " +
                    "WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                ps.setLong(1, snapshotId);
                ps.setLong(2, snapshotId);
                try (ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();
                    while (rs.next()) {
                        Map<String, String> row = new LinkedHashMap<>();
                        for (int i = 1; i <= colCount; i++) {
                            String colName = meta.getColumnName(i);
                            if (!"row_id".equals(colName) &&
                                !"begin_snapshot".equals(colName) &&
                                !"end_snapshot".equals(colName)) {
                                row.put(colName, rs.getString(i));
                            }
                        }
                        allRows.add(row);
                    }
                }
            } catch (SQLException e) {
                // Table may not exist
            }
        }
        return allRows;
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private String ensureInlinedTable(Connection conn, long tableId, long schemaVersion,
                                      List<ColumnInfo> columns) throws SQLException {
        String tblName = "ducklake_inlined_data_" + tableId + "_" + schemaVersion;
        String safeName = tblName.replace("\"", "\"\"");

        // Ensure registry table exists
        ensureRegistryTable(conn);

        // Check if already registered
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT table_name FROM ducklake_inlined_data_tables " +
                "WHERE table_id = ? AND schema_version = ?")) {
            ps.setLong(1, tableId);
            ps.setLong(2, schemaVersion);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return tblName;
                }
            }
        }

        // Create the inlined data table
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"").append(safeName).append("\" (");
        sb.append("row_id BIGINT, ");
        sb.append("begin_snapshot BIGINT, ");
        sb.append("end_snapshot BIGINT");
        for (ColumnInfo col : columns) {
            sb.append(", \"").append(col.name.replace("\"", "\"\"")).append("\" ");
            sb.append(mapToStorageType(col.type));
        }
        sb.append(")");

        try (Statement st = conn.createStatement()) {
            st.execute(sb.toString());
        }

        // Register it
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ducklake_inlined_data_tables (table_id, table_name, schema_version) " +
                "VALUES (?, ?, ?)")) {
            ps.setLong(1, tableId);
            ps.setString(2, tblName);
            ps.setLong(3, schemaVersion);
            ps.executeUpdate();
        }

        return tblName;
    }

    private void ensureRegistryTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS ducklake_inlined_data_tables (" +
                    "table_id BIGINT, table_name VARCHAR, schema_version BIGINT)");
        }
    }

    private List<String> getInlinedTableNames(Connection conn, long tableId) throws SQLException {
        List<String> names = new ArrayList<>();
        ensureRegistryTable(conn);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id = ?")) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        }
        return names;
    }

    /**
     * Map DuckDB column types to SQLite/PostgreSQL-compatible storage types.
     */
    private static String mapToStorageType(String duckdbType) {
        String t = duckdbType.toLowerCase().trim();
        if (t.matches("(tiny|small|)int(eger)?|bigint|int8|int16|int32|int64|uint8|uint16|uint32|uint64|boolean")) {
            return "BIGINT";
        }
        if (t.matches("float|double|float32|float64|real")) {
            return "DOUBLE";
        }
        return "VARCHAR";
    }

    private static int findFieldIndex(StructType schema, String colName) {
        StructField[] fields = schema.fields();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].name().equalsIgnoreCase(colName)) {
                return i;
            }
        }
        return -1;
    }

    private static Object extractValue(InternalRow row, int ordinal, DataType type) {
        if (type instanceof BooleanType) return row.getBoolean(ordinal);
        if (type instanceof ByteType) return row.getByte(ordinal);
        if (type instanceof ShortType) return row.getShort(ordinal);
        if (type instanceof IntegerType) return row.getInt(ordinal);
        if (type instanceof LongType) return row.getLong(ordinal);
        if (type instanceof FloatType) return row.getFloat(ordinal);
        if (type instanceof DoubleType) return row.getDouble(ordinal);
        if (type instanceof StringType) return row.getUTF8String(ordinal).toString();
        if (type instanceof DateType) return row.getInt(ordinal);
        if (type instanceof TimestampType) return row.getLong(ordinal);
        if (type instanceof DecimalType) {
            DecimalType dt = (DecimalType) type;
            return row.getDecimal(ordinal, dt.precision(), dt.scale()).toJavaBigDecimal();
        }
        return row.get(ordinal, type);
    }

    private static String serializeValue(Object value, String colType) {
        if (value == null) return null;
        String t = colType.toLowerCase();
        if (t.equals("boolean")) {
            return ((Boolean) value) ? "1" : "0";
        }
        if (t.equals("date")) {
            if (value instanceof Integer) {
                return LocalDate.ofEpochDay((Integer) value).toString();
            }
        }
        if (t.startsWith("timestamp")) {
            if (value instanceof Long) {
                long micros = (Long) value;
                Instant instant = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
                return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toString();
            }
        }
        return value.toString();
    }
}
