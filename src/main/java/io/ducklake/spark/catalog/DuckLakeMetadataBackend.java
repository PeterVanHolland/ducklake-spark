package io.ducklake.spark.catalog;

import java.sql.*;
import java.util.*;
import java.util.ConcurrentModificationException;

/**
 * Reads DuckLake catalog metadata from a SQL database (SQLite, DuckDB, or PostgreSQL).
 * This is the core abstraction over the DuckLake metadata schema.
 *
 * <p>Backend detection logic (matches ducklake-dataframe reference implementation):</p>
 * <ul>
 *   <li><b>PostgreSQL</b>: path starts with {@code postgresql://}, {@code postgres://},
 *       or {@code jdbc:postgresql:}</li>
 *   <li><b>DuckDB</b>: path ends with {@code .duckdb}, starts with {@code duckdb:},
 *       or starts with {@code jdbc:duckdb:}</li>
 *   <li><b>SQLite</b>: everything else (default)</li>
 * </ul>
 *
 * <p>DuckDB requires the DuckDB JDBC driver ({@code org.duckdb:duckdb_jdbc}) on the
 * classpath at runtime. When using Spark, add it via {@code --jars} or
 * {@code spark.jars.packages}.</p>
 */
public class DuckLakeMetadataBackend implements AutoCloseable {
    private final String jdbcUrl;
    private final String dataPath;
    private Connection connection;
    private boolean ownsConnection = true;

    // Connection pooling for repeated open/close on same catalog
    private static final java.util.concurrent.ConcurrentHashMap<String, Connection> connectionPool =
            new java.util.concurrent.ConcurrentHashMap<>();

    public DuckLakeMetadataBackend(String catalogPath, String dataPath) {
        if (catalogPath.startsWith("postgresql://") || catalogPath.startsWith("postgres://")
                || catalogPath.startsWith("jdbc:postgresql:")) {
            this.jdbcUrl = catalogPath.startsWith("jdbc:") ? catalogPath : "jdbc:" + catalogPath;
        } else if (catalogPath.endsWith(".duckdb") || catalogPath.startsWith("duckdb:")
                || catalogPath.startsWith("jdbc:duckdb:")) {
            // DuckDB backend
            if (catalogPath.startsWith("jdbc:duckdb:")) {
                this.jdbcUrl = catalogPath;
            } else if (catalogPath.startsWith("duckdb:")) {
                this.jdbcUrl = "jdbc:duckdb:" + catalogPath.substring("duckdb:".length());
            } else {
                this.jdbcUrl = "jdbc:duckdb:" + catalogPath;
            }
        } else {
            // SQLite (default)
            this.jdbcUrl = "jdbc:sqlite:" + catalogPath;
        }
        this.dataPath = dataPath;
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            // Try to reuse a pooled connection
            Connection pooled = connectionPool.remove(jdbcUrl);
            if (pooled != null && !pooled.isClosed()) {
                connection = pooled;
                ownsConnection = true;
            } else {
                connection = DriverManager.getConnection(jdbcUrl);
                ownsConnection = true;
            }
        }
        return connection;
    }

    /** Expose the underlying connection for view/tag/inline operations. */
    /** Expose the underlying connection for view/tag/inline operations. */
    public Connection getConnectionForInlining() throws SQLException {
        return getConnection();
    }

    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed() && ownsConnection) {
            // Return to pool instead of closing
            connectionPool.put(jdbcUrl, connection);
            connection = null;
        }
    }

    /** Actually close the connection (bypass pool). */
    public void closeForReal() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            connection = null;
        }
    }


    /**
     * Read a boolean column from a ResultSet in a way that works across
     * SQLite (stores as 0/1 INTEGER) and DuckDB (stores as BOOLEAN).
     */
    private static boolean getBooleanCompat(ResultSet rs, String column) throws SQLException {
        try {
            return rs.getBoolean(column);
        } catch (SQLException e) {
            // Fallback for drivers that don't support getBoolean on integer columns
            return rs.getInt(column) == 1;
        }
    }

    private static boolean getBooleanCompat(ResultSet rs, int columnIndex) throws SQLException {
        try {
            return rs.getBoolean(columnIndex);
        } catch (SQLException e) {
            return rs.getInt(columnIndex) == 1;
        }
    }

    // ---------------------------------------------------------------
    // Metadata queries
    // ---------------------------------------------------------------

    /** Get catalog metadata value by key. */
    public String getMetadata(String key) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT value FROM ducklake_metadata WHERE key = ? AND scope IS NULL")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /** Get the data path from metadata or the configured path. */
    public String getDataPath() throws SQLException {
        if (dataPath != null && !dataPath.isEmpty()) {
            return dataPath;
        }
        return getMetadata("data_path");
    }

    /** Get the current (latest) snapshot ID. */
    public long getCurrentSnapshotId() throws SQLException {
        try (Statement s = getConnection().createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT MAX(snapshot_id) FROM ducklake_snapshot")) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    // ---------------------------------------------------------------
    // Snapshot queries (time travel)
    // ---------------------------------------------------------------

    /** Get snapshot by version (snapshot_id). Returns null if not found. */
    public SnapshotInfo getSnapshotAtVersion(long version) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT s.snapshot_id, s.snapshot_time, c.changes_made " +
                "FROM ducklake_snapshot s " +
                "LEFT JOIN ducklake_snapshot_changes c ON s.snapshot_id = c.snapshot_id " +
                "WHERE s.snapshot_id = ?")) {
            ps.setLong(1, version);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SnapshotInfo(
                            rs.getLong("snapshot_id"),
                            rs.getString("snapshot_time"),
                            rs.getString("changes_made"));
                }
            }
        }
        return null;
    }

    /** Get the latest snapshot at or before the given timestamp. Returns null if none found. */
    public SnapshotInfo getSnapshotAtTime(String timestamp) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT s.snapshot_id, s.snapshot_time, c.changes_made " +
                "FROM ducklake_snapshot s " +
                "LEFT JOIN ducklake_snapshot_changes c ON s.snapshot_id = c.snapshot_id " +
                "WHERE s.snapshot_time <= ? ORDER BY s.snapshot_id DESC LIMIT 1")) {
            ps.setString(1, timestamp);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SnapshotInfo(
                            rs.getLong("snapshot_id"),
                            rs.getString("snapshot_time"),
                            rs.getString("changes_made"));
                }
            }
        }
        return null;
    }

    /** List all snapshots ordered by version (ascending). */
    public List<SnapshotInfo> listSnapshots() throws SQLException {
        List<SnapshotInfo> result = new ArrayList<>();
        try (Statement s = getConnection().createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT s.snapshot_id, s.snapshot_time, c.changes_made " +
                     "FROM ducklake_snapshot s " +
                     "LEFT JOIN ducklake_snapshot_changes c ON s.snapshot_id = c.snapshot_id " +
                     "ORDER BY s.snapshot_id")) {
            while (rs.next()) {
                result.add(new SnapshotInfo(
                        rs.getLong("snapshot_id"),
                        rs.getString("snapshot_time"),
                        rs.getString("changes_made")));
            }
        }
        return result;
    }

    /**
     * Resolve a snapshot ID from version/time options.
     * Returns the current (latest) snapshot if neither is specified.
     *
     * @param snapshotVersion version string (snapshot_id), or null
     * @param snapshotTime    ISO-8601 timestamp string, or null
     * @throws IllegalArgumentException if both are set, or if the target snapshot is not found
     */
    public long resolveSnapshotId(String snapshotVersion, String snapshotTime) throws SQLException {
        if (snapshotVersion != null && snapshotTime != null) {
            throw new IllegalArgumentException(
                    "Cannot specify both 'snapshot_version' and 'snapshot_time' -- use one or the other");
        }
        if (snapshotVersion != null) {
            long version;
            try {
                version = Long.parseLong(snapshotVersion);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid snapshot_version: " + snapshotVersion + " -- must be a numeric version");
            }
            SnapshotInfo snap = getSnapshotAtVersion(version);
            if (snap == null) {
                throw new IllegalArgumentException("Snapshot version " + version + " not found");
            }
            return snap.snapshotId;
        }
        if (snapshotTime != null) {
            SnapshotInfo snap = getSnapshotAtTime(snapshotTime);
            if (snap == null) {
                throw new IllegalArgumentException(
                        "No snapshot found at or before timestamp: " + snapshotTime);
            }
            return snap.snapshotId;
        }
        return getCurrentSnapshotId();
    }

    // ---------------------------------------------------------------
    // Schema queries
    // ---------------------------------------------------------------

    /** List all schemas visible at the current snapshot. */
    public List<SchemaInfo> listSchemas() throws SQLException {
        long snap = getCurrentSnapshotId();
        List<SchemaInfo> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT schema_id, schema_name, path, path_is_relative " +
                "FROM ducklake_schema " +
                "WHERE begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setLong(1, snap);
            ps.setLong(2, snap);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new SchemaInfo(
                            rs.getLong("schema_id"),
                            rs.getString("schema_name"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative")));
                }
            }
        }
        return result;
    }

    // ---------------------------------------------------------------
    // Table queries
    // ---------------------------------------------------------------

    /** List tables in a schema at the current snapshot. */
    public List<TableInfo> listTables(long schemaId) throws SQLException {
        long snap = getCurrentSnapshotId();
        List<TableInfo> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT table_id, table_uuid, table_name, path, path_is_relative " +
                "FROM ducklake_table " +
                "WHERE schema_id = ? AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setLong(1, schemaId);
            ps.setLong(2, snap);
            ps.setLong(3, snap);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new TableInfo(
                            rs.getLong("table_id"),
                            rs.getString("table_uuid"),
                            rs.getString("table_name"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative")));
                }
            }
        }
        return result;
    }

    /** Get a table by name in a schema. */
    public TableInfo getTable(long schemaId, String tableName) throws SQLException {
        long snap = getCurrentSnapshotId();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT table_id, table_uuid, table_name, path, path_is_relative " +
                "FROM ducklake_table " +
                "WHERE schema_id = ? AND table_name = ? AND begin_snapshot <= ? " +
                "AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setLong(1, schemaId);
            ps.setString(2, tableName);
            ps.setLong(3, snap);
            ps.setLong(4, snap);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new TableInfo(
                            rs.getLong("table_id"),
                            rs.getString("table_uuid"),
                            rs.getString("table_name"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"));
                }
            }
        }
        return null;
    }

    /** Get a table by name (searches "main" schema by default). */
    public TableInfo getTable(String tableName) throws SQLException {
        return getTable(tableName, "main");
    }

    /** Get a table by name in a named schema at the current snapshot. */
    public TableInfo getTable(String tableName, String schemaName) throws SQLException {
        return getTable(tableName, schemaName, getCurrentSnapshotId());
    }

    /** Get a table by name in a named schema at a specific snapshot. */
    /** Get table directly by ID (skip schema/name resolution). */
    public TableInfo getTableById(long tableId, long snapshotId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT table_id, table_uuid, table_name, path, path_is_relative " +
                "FROM ducklake_table WHERE table_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setLong(1, tableId);
            ps.setLong(2, snapshotId);
            ps.setLong(3, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new TableInfo(
                            rs.getLong("table_id"),
                            rs.getString("table_uuid"),
                            rs.getString("table_name"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"));
                }
            }
        }
        return null;
    }

    public TableInfo getTable(String tableName, String schemaName, long snapshotId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT t.table_id, t.table_uuid, t.table_name, t.path, t.path_is_relative " +
                "FROM ducklake_table t " +
                "JOIN ducklake_schema s ON t.schema_id = s.schema_id " +
                "WHERE s.schema_name = ? AND t.table_name = ? " +
                "AND t.begin_snapshot <= ? AND (t.end_snapshot IS NULL OR t.end_snapshot > ?) " +
                "AND s.begin_snapshot <= ? AND (s.end_snapshot IS NULL OR s.end_snapshot > ?)")) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            ps.setLong(3, snapshotId);
            ps.setLong(4, snapshotId);
            ps.setLong(5, snapshotId);
            ps.setLong(6, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new TableInfo(
                            rs.getLong("table_id"),
                            rs.getString("table_uuid"),
                            rs.getString("table_name"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"));
                }
            }
        }
        return null;
    }

    // ---------------------------------------------------------------
    // Column queries
    // ---------------------------------------------------------------

    /** Get columns for a table at the current snapshot (top-level only). */
    public List<ColumnInfo> getColumns(long tableId) throws SQLException {
        return getColumns(tableId, getCurrentSnapshotId());
    }

    /** Get columns for a table at a specific snapshot. */
    public List<ColumnInfo> getColumns(long tableId, long snapshotId) throws SQLException {
        List<ColumnInfo> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT column_id, column_name, column_type, column_order, " +
                "initial_default, default_value, nulls_allowed, parent_column " +
                "FROM ducklake_column " +
                "WHERE table_id = ? AND parent_column IS NULL " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY column_order")) {
            ps.setLong(1, tableId);
            ps.setLong(2, snapshotId);
            ps.setLong(3, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new ColumnInfo(
                            rs.getLong("column_id"),
                            rs.getString("column_name"),
                            rs.getString("column_type"),
                            rs.getInt("column_order"),
                            rs.getString("initial_default"),
                            rs.getString("default_value"),
                            getBooleanCompat(rs, "nulls_allowed"),
                            rs.getObject("parent_column") == null ? -1 : rs.getLong("parent_column")));
                }
            }
        }
        return result;
    }

    // ---------------------------------------------------------------
    // Data file queries
    // ---------------------------------------------------------------

    /** Get active data files for a table at the current snapshot. */
    public List<DataFileInfo> getDataFiles(long tableId) throws SQLException {
        return getDataFiles(tableId, getCurrentSnapshotId());
    }

    /** Get active data files for a table at a specific snapshot. */
    public List<DataFileInfo> getDataFiles(long tableId, long snapshotId) throws SQLException {
        List<DataFileInfo> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT data_file_id, path, path_is_relative, file_format, " +
                "record_count, file_size_bytes, mapping_id, partition_id " +
                "FROM ducklake_data_file " +
                "WHERE table_id = ? AND begin_snapshot <= ? " +
                "AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY file_order")) {
            ps.setLong(1, tableId);
            ps.setLong(2, snapshotId);
            ps.setLong(3, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new DataFileInfo(
                            rs.getLong("data_file_id"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"),
                            rs.getString("file_format"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes"),
                            rs.getObject("mapping_id") == null ? -1 : rs.getLong("mapping_id"),
                            rs.getObject("partition_id") == null ? -1 : rs.getLong("partition_id")));
                }
            }
        }
        return result;
    }

    /** Get active delete files for a data file at the current snapshot. */
    public List<DeleteFileInfo> getDeleteFiles(long tableId, long dataFileId) throws SQLException {
        return getDeleteFiles(tableId, dataFileId, getCurrentSnapshotId());
    }

    /** Get active delete files for a data file at a specific snapshot. */
    public List<DeleteFileInfo> getDeleteFiles(long tableId, long dataFileId, long snapshotId) throws SQLException {
        List<DeleteFileInfo> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT delete_file_id, path, path_is_relative, format, delete_count " +
                "FROM ducklake_delete_file " +
                "WHERE table_id = ? AND data_file_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setLong(1, tableId);
            ps.setLong(2, dataFileId);
            ps.setLong(3, snapshotId);
            ps.setLong(4, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new DeleteFileInfo(
                            rs.getLong("delete_file_id"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"),
                            rs.getString("format"),
                            rs.getLong("delete_count")));
                }
            }
        }
        return result;
    }

    /** Get data files added between snapshots (for CDC). */
    public List<DataFileInfo> getDataFilesInRange(long tableId, long fromSnapshotExclusive, long toSnapshotInclusive) throws SQLException {
        List<DataFileInfo> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT data_file_id, path, path_is_relative, file_format, " +
                "record_count, file_size_bytes, mapping_id, partition_id, begin_snapshot " +
                "FROM ducklake_data_file " +
                "WHERE table_id = ? AND begin_snapshot > ? AND begin_snapshot <= ? " +
                "ORDER BY begin_snapshot, file_order")) {
            ps.setLong(1, tableId);
            ps.setLong(2, fromSnapshotExclusive);
            ps.setLong(3, toSnapshotInclusive);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new DataFileInfo(
                            rs.getLong("data_file_id"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"),
                            rs.getString("file_format"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes"),
                            rs.getObject("mapping_id") == null ? -1 : rs.getLong("mapping_id"),
                            rs.getObject("partition_id") == null ? -1 : rs.getLong("partition_id"),
                            rs.getLong("begin_snapshot")));
                }
            }
        }
        return result;
    }

    /** Get delete files added between snapshots (for CDC). */
    public List<DeleteFileInfo> getDeleteFilesInRange(long tableId, long fromSnapshotExclusive, long toSnapshotInclusive) throws SQLException {
        List<DeleteFileInfo> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT delete_file_id, data_file_id, path, path_is_relative, format, delete_count, begin_snapshot " +
                "FROM ducklake_delete_file " +
                "WHERE table_id = ? AND begin_snapshot > ? AND begin_snapshot <= ? " +
                "ORDER BY begin_snapshot")) {
            ps.setLong(1, tableId);
            ps.setLong(2, fromSnapshotExclusive);
            ps.setLong(3, toSnapshotInclusive);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new DeleteFileInfo(
                            rs.getLong("delete_file_id"),
                            rs.getLong("data_file_id"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"),
                            rs.getString("format"),
                            rs.getLong("delete_count"),
                            rs.getLong("begin_snapshot")));
                }
            }
        }
        return result;
    }

    /** Get a single data file by its ID. */
    public DataFileInfo getDataFileById(long dataFileId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT data_file_id, path, path_is_relative, file_format, record_count, " +
                "file_size_bytes, COALESCE(mapping_id, -1), COALESCE(partition_id, -1), " +
                "begin_snapshot, COALESCE(row_id_start, 0) " +
                "FROM ducklake_data_file WHERE data_file_id = ?")) {
            ps.setLong(1, dataFileId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new DataFileInfo(
                            rs.getLong(1), rs.getString(2), getBooleanCompat(rs, 3),
                            rs.getString(4), rs.getLong(5), rs.getLong(6),
                            rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getLong(10));
                }
                return null;
            }
        }
    }

    // ---------------------------------------------------------------
    // Statistics queries
    // ---------------------------------------------------------------

    /** Get per-file column statistics for predicate pushdown. */
    public List<FileColumnStats> getFileColumnStats(long tableId, long dataFileId) throws SQLException {
        List<FileColumnStats> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT column_id, min_value, max_value, null_count, value_count " +
                "FROM ducklake_file_column_stats " +
                "WHERE table_id = ? AND data_file_id = ?")) {
            ps.setLong(1, tableId);
            ps.setLong(2, dataFileId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new FileColumnStats(
                            rs.getLong("column_id"),
                            rs.getString("min_value"),
                            rs.getString("max_value"),
                            rs.getLong("null_count"),
                            rs.getLong("value_count")));
                }
            }
        }
        return result;
    }

    /** Get partition values for a data file. */
    public Map<Integer, String> getPartitionValues(long tableId, long dataFileId) throws SQLException {
        Map<Integer, String> result = new TreeMap<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT partition_key_index, partition_value " +
                "FROM ducklake_file_partition_value " +
                "WHERE table_id = ? AND data_file_id = ?")) {
            ps.setLong(1, tableId);
            ps.setLong(2, dataFileId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getInt("partition_key_index"),
                               rs.getString("partition_value"));
                }
            }
        }
        return result;
    }

    /** Get partition column information for a table. */
    public List<PartitionInfo> getPartitionColumns(long tableId, long snapshotId) throws SQLException {
        List<PartitionInfo> result = new ArrayList<>();

        // Query the catalog for partition columns
        // First try to use ducklake_table_partition_column if it exists
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT c.column_id, c.column_name, " +
                "CASE WHEN nm.is_partition = 1 THEN tpc.partition_expression ELSE NULL END as partition_expression, " +
                "CASE WHEN nm.is_partition = 1 THEN ROW_NUMBER() OVER (ORDER BY c.column_order) - 1 ELSE -1 END as partition_index " +
                "FROM ducklake_column c " +
                "LEFT JOIN ducklake_name_mapping nm ON nm.column_id = c.column_id " +
                "LEFT JOIN ducklake_table_partition_column tpc ON tpc.column_id = c.column_id AND tpc.table_id = ? " +
                "WHERE c.table_id = ? AND c.end_snapshot IS NULL " +
                "AND (c.begin_snapshot <= ? AND (c.end_snapshot IS NULL OR c.end_snapshot > ?)) " +
                "AND nm.is_partition = 1 " +
                "ORDER BY c.column_order")) {

            ps.setLong(1, tableId);
            ps.setLong(2, tableId);
            ps.setLong(3, snapshotId);
            ps.setLong(4, snapshotId);

            try (ResultSet rs = ps.executeQuery()) {
                int partitionIndex = 0;
                while (rs.next()) {
                    result.add(new PartitionInfo(
                            rs.getLong("column_id"),
                            rs.getString("column_name"),
                            rs.getString("partition_expression"),
                            partitionIndex++
                    ));
                }
            }
        } catch (SQLException e) {
            // If ducklake_table_partition_column doesn't exist, fall back to simple is_partition check
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT c.column_id, c.column_name " +
                    "FROM ducklake_column c " +
                    "JOIN ducklake_name_mapping nm ON nm.column_id = c.column_id " +
                    "WHERE c.table_id = ? " +
                    "AND (c.begin_snapshot <= ? AND (c.end_snapshot IS NULL OR c.end_snapshot > ?)) " +
                    "AND nm.is_partition = 1 " +
                    "ORDER BY c.column_order")) {

                ps.setLong(1, tableId);
                ps.setLong(2, snapshotId);
                ps.setLong(3, snapshotId);

                try (ResultSet rs = ps.executeQuery()) {
                    int partitionIndex = 0;
                    while (rs.next()) {
                        result.add(new PartitionInfo(
                                rs.getLong("column_id"),
                                rs.getString("column_name"),
                                null,  // identity partition
                                partitionIndex++
                        ));
                    }
                }
            }
        }

        return result;
    }

    /** Get data files that match partition filters. */
    public List<DataFileInfo> getDataFilesForPartition(long tableId, long snapshotId,
                                                       List<PartitionFilter> partitionFilters) throws SQLException {
        if (partitionFilters == null || partitionFilters.isEmpty()) {
            return getDataFiles(tableId, snapshotId);
        }

        List<DataFileInfo> result = new ArrayList<>();

        // Build WHERE clause for partition filtering
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Add base filters
        whereClause.append("df.table_id = ? AND df.begin_snapshot <= ? AND (df.end_snapshot IS NULL OR df.end_snapshot > ?)");
        params.add(tableId);
        params.add(snapshotId);
        params.add(snapshotId);

        // Add partition filters
        for (PartitionFilter filter : partitionFilters) {
            whereClause.append(" AND EXISTS (")
                      .append("SELECT 1 FROM ducklake_file_partition_value fpv ")
                      .append("WHERE fpv.data_file_id = df.data_file_id ")
                      .append("AND fpv.table_id = df.table_id ")
                      .append("AND fpv.partition_key_index = ? ");
            params.add(filter.partitionIndex);

            switch (filter.operator) {
                case "=":
                    whereClause.append("AND fpv.partition_value = ?");
                    params.add(filter.value.toString());
                    break;
                case "IN":
                    @SuppressWarnings("unchecked")
                    List<Object> values = (List<Object>) filter.value;
                    whereClause.append("AND fpv.partition_value IN (");
                    for (int i = 0; i < values.size(); i++) {
                        if (i > 0) whereClause.append(",");
                        whereClause.append("?");
                        params.add(values.get(i).toString());
                    }
                    whereClause.append(")");
                    break;
                case "<":
                    whereClause.append("AND fpv.partition_value < ?");
                    params.add(filter.value.toString());
                    break;
                case ">":
                    whereClause.append("AND fpv.partition_value > ?");
                    params.add(filter.value.toString());
                    break;
                case "<=":
                    whereClause.append("AND fpv.partition_value <= ?");
                    params.add(filter.value.toString());
                    break;
                case ">=":
                    whereClause.append("AND fpv.partition_value >= ?");
                    params.add(filter.value.toString());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported partition filter operator: " + filter.operator);
            }

            whereClause.append(")");
        }

        String sql = "SELECT df.data_file_id, df.path, df.path_is_relative, df.file_format, " +
                     "df.record_count, df.file_size_bytes, df.mapping_id, df.partition_id " +
                     "FROM ducklake_data_file df " +
                     "WHERE " + whereClause.toString() +
                     " ORDER BY df.file_order";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new DataFileInfo(
                            rs.getLong("data_file_id"),
                            rs.getString("path"),
                            rs.getBoolean("path_is_relative"),
                            rs.getString("file_format"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes"),
                            rs.getObject("mapping_id") == null ? -1 : rs.getLong("mapping_id"),
                            rs.getObject("partition_id") == null ? -1 : rs.getLong("partition_id")));
                }
            }
        }

        return result;
    }

    /** Get name mappings for a mapping ID (for column renames). */
    public Map<Long, String> getNameMapping(long mappingId) throws SQLException {
        Map<Long, String> result = new HashMap<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT target_field_id, source_name FROM ducklake_name_mapping " +
                "WHERE mapping_id = ?")) {
            ps.setLong(1, mappingId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getLong("target_field_id"),
                               rs.getString("source_name"));
                }
            }
        }
        return result;
    }

    /** Get inlined data rows for a table. */
    public List<Map<String, String>> getInlinedData(long tableId) throws SQLException {
        // First check if inlined data table exists
        String tableName = null;
        long schemaVersion = -1;
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT table_name, schema_version FROM ducklake_inlined_data_tables " +
                "WHERE table_id = ?")) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    tableName = rs.getString("table_name");
                    schemaVersion = rs.getLong("schema_version");
                } else {
                    return Collections.emptyList();
                }
            }
        }

        // Read inlined data
        List<Map<String, String>> rows = new ArrayList<>();
        try (Statement s = getConnection().createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnName(i), rs.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    // ---------------------------------------------------------------
    // Write operations
    // ---------------------------------------------------------------

    /** Begin a transaction on the underlying connection. */
    public void beginTransaction() throws SQLException {
        getConnection().setAutoCommit(false);
    }

    /** Commit the current transaction. */
    public void commitTransaction() throws SQLException {
        getConnection().commit();
        getConnection().setAutoCommit(true);
        DuckLakeCatalog.invalidateMetadataCache();
    }

    /** Rollback the current transaction. */
    public void rollbackTransaction() throws SQLException {
        try {
            getConnection().rollback();
        } finally {
            getConnection().setAutoCommit(true);
        }
    }


    /** Get schema version at a specific snapshot. */
    public long getSchemaVersion(long snapshotId) throws SQLException {
        CatalogState state = getSnapshotInfo(snapshotId);
        return state.schemaVersion;
    }
    /** Get snapshot metadata. */
    public CatalogState getSnapshotInfo(long snapshotId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT schema_version, next_catalog_id, next_file_id FROM ducklake_snapshot WHERE snapshot_id = ?")) {
            ps.setLong(1, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new CatalogState(
                            rs.getLong("schema_version"),
                            rs.getLong("next_catalog_id"),
                            rs.getLong("next_file_id"));
                }
            }
        }
        throw new SQLException("Snapshot not found: " + snapshotId);
    }

    /** Get table-level statistics. */
    public TableStats getTableStats(long tableId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats WHERE table_id = ?")) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new TableStats(
                            rs.getLong("record_count"),
                            rs.getLong("next_row_id"),
                            rs.getLong("file_size_bytes"));
                }
            }
        }
        return new TableStats(0, 0, 0);
    }

    /** Check if a table has any active data files. */
    public boolean hasDataFiles(long tableId) throws SQLException {
        long snap = getCurrentSnapshotId();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT 1 FROM ducklake_data_file WHERE table_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) LIMIT 1")) {
            ps.setLong(1, tableId);
            ps.setLong(2, snap);
            ps.setLong(3, snap);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** Create a new snapshot record. */
    public void createSnapshot(long snapshotId, long schemaVersion, long nextCatalogId, long nextFileId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES (?, ?, ?, ?, ?)")) {
            ps.setLong(1, snapshotId);
            ps.setString(2, java.time.LocalDateTime.now(java.time.ZoneOffset.UTC).toString());
            ps.setLong(3, schemaVersion);
            ps.setLong(4, nextCatalogId);
            ps.setLong(5, nextFileId);
            ps.executeUpdate();
        }
    }

    /**
     * Creates a new snapshot atomically with optimistic concurrency control.
     * Verifies that the expected current snapshot ID matches before creating the new snapshot.
     *
     * @param expectedCurrentSnapshot The snapshot ID that should currently be the maximum
     * @param snapshotId The new snapshot ID to create
     * @param schemaVersion Schema version for the new snapshot
     * @param nextCatalogId Next catalog ID for the new snapshot
     * @param nextFileId Next file ID for the new snapshot
     * @throws java.util.ConcurrentModificationException if the expected current snapshot doesn't match
     */
    public void createSnapshotAtomically(long expectedCurrentSnapshot, long snapshotId,
            long schemaVersion, long nextCatalogId, long nextFileId) throws SQLException {
        // Verify current max snapshot matches expectation
        long actualCurrentSnapshot = getCurrentSnapshotId();
        if (actualCurrentSnapshot != expectedCurrentSnapshot) {
            throw new java.util.ConcurrentModificationException(
                    "Snapshot conflict: expected " + expectedCurrentSnapshot +
                    " but found " + actualCurrentSnapshot);
        }

        // Create the new snapshot
        createSnapshot(snapshotId, schemaVersion, nextCatalogId, nextFileId);
    }

    /** Insert a snapshot changes record. */
    public void insertSnapshotChanges(long snapshotId, String changesMade, String author, String commitMessage) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made, author, commit_message) VALUES (?, ?, ?, ?)")) {
            ps.setLong(1, snapshotId);
            ps.setString(2, changesMade);
            ps.setString(3, author);
            ps.setString(4, commitMessage);
            ps.executeUpdate();
        }
    }

    /** Insert a data file record. */
    public void insertDataFile(long dataFileId, long tableId, long beginSnapshot, long fileOrder,
                               String path, long recordCount, long fileSizeBytes, long rowIdStart) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, " +
                "file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max) " +
                "VALUES (?, ?, ?, NULL, ?, ?, 1, 'PARQUET', ?, ?, 0, ?, NULL, NULL, NULL, NULL)")) {
            ps.setLong(1, dataFileId);
            ps.setLong(2, tableId);
            ps.setLong(3, beginSnapshot);
            ps.setLong(4, fileOrder);
            ps.setString(5, path);
            ps.setLong(6, recordCount);
            ps.setLong(7, fileSizeBytes);
            ps.setLong(8, rowIdStart);
            ps.executeUpdate();
        }
    }

    /** Insert partition values for a data file. */
    public void insertPartitionValues(long dataFileId, long tableId,
                                     Map<Integer, String> partitionValues) throws SQLException {
        if (partitionValues == null || partitionValues.isEmpty()) {
            return;
        }

        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value) " +
                "VALUES (?, ?, ?, ?)")) {
            for (Map.Entry<Integer, String> entry : partitionValues.entrySet()) {
                ps.setLong(1, dataFileId);
                ps.setLong(2, tableId);
                ps.setInt(3, entry.getKey());
                ps.setString(4, entry.getValue());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Insert a data file record with explicit path_is_relative flag (for add_files). */
    public void insertDataFileAbsolute(long dataFileId, long tableId, long beginSnapshot, long fileOrder,
                                        String path, boolean pathIsRelative, long recordCount, long fileSizeBytes,
                                        long rowIdStart) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, " +
                "file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max) " +
                "VALUES (?, ?, ?, NULL, ?, ?, ?, 'PARQUET', ?, ?, 0, ?, NULL, NULL, NULL, NULL)")) {
            ps.setLong(1, dataFileId);
            ps.setLong(2, tableId);
            ps.setLong(3, beginSnapshot);
            ps.setLong(4, fileOrder);
            ps.setString(5, path);
            ps.setInt(6, pathIsRelative ? 1 : 0);
            ps.setLong(7, recordCount);
            ps.setLong(8, fileSizeBytes);
            ps.setLong(9, rowIdStart);
            ps.executeUpdate();
        }
    }

    /** Batch insert column statistics for multiple columns of a data file. */
    public void insertColumnStatsBatch(long dataFileId, long tableId,
                                       List<long[]> statsData, List<String[]> statsStrings) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, " +
                "min_value, max_value, contains_nan, extra_stats) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, NULL, NULL)")) {
            for (int i = 0; i < statsData.size(); i++) {
                long[] nums = statsData.get(i);
                String[] strs = statsStrings.get(i);
                ps.setLong(1, dataFileId);
                ps.setLong(2, tableId);
                ps.setLong(3, nums[0]); // columnId
                ps.setLong(4, nums[1]); // valueCount
                ps.setLong(5, nums[2]); // nullCount
                if (strs[0] != null) { ps.setString(6, strs[0]); } else { ps.setNull(6, java.sql.Types.VARCHAR); }
                if (strs[1] != null) { ps.setString(7, strs[1]); } else { ps.setNull(7, java.sql.Types.VARCHAR); }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Insert column statistics for a data file. */
    public void insertColumnStats(long dataFileId, long tableId, long columnId,
                                  long valueCount, long nullCount, String minValue, String maxValue) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, " +
                "min_value, max_value, contains_nan, extra_stats) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, NULL, NULL)")) {
            ps.setLong(1, dataFileId);
            ps.setLong(2, tableId);
            ps.setLong(3, columnId);
            ps.setLong(4, valueCount);
            ps.setLong(5, nullCount);
            if (minValue != null) { ps.setString(6, minValue); } else { ps.setNull(6, java.sql.Types.VARCHAR); }
            if (maxValue != null) { ps.setString(7, maxValue); } else { ps.setNull(7, java.sql.Types.VARCHAR); }
            ps.executeUpdate();
        }
    }

    /** Upsert table-level statistics. */
    public void updateTableStats(long tableId, long recordCount, long nextRowId, long fileSizeBytes) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "UPDATE ducklake_table_stats SET record_count = ?, next_row_id = ?, file_size_bytes = ? WHERE table_id = ?")) {
            ps.setLong(1, recordCount);
            ps.setLong(2, nextRowId);
            ps.setLong(3, fileSizeBytes);
            ps.setLong(4, tableId);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                try (PreparedStatement insert = getConnection().prepareStatement(
                        "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) VALUES (?, ?, ?, ?)")) {
                    insert.setLong(1, tableId);
                    insert.setLong(2, recordCount);
                    insert.setLong(3, nextRowId);
                    insert.setLong(4, fileSizeBytes);
                    insert.executeUpdate();
                }
            }
        }
    }

    /** Mark all active data files for a table as deleted at the given snapshot. */
    public void markDataFilesDeleted(long tableId, long snapshotId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "UPDATE ducklake_data_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
            ps.setLong(1, snapshotId);
            ps.setLong(2, tableId);
            ps.executeUpdate();
        }
    }

    /** Insert a delete file record. */
    public void insertDeleteFile(long deleteFileId, long tableId, long beginSnapshot,
                                  long dataFileId, String path, long deleteCount,
                                  long fileSizeBytes) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, " +
                "data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, " +
                "footer_size, encryption_key, partial_max) " +
                "VALUES (?, ?, ?, NULL, ?, ?, 1, 'PARQUET', ?, ?, 0, NULL, NULL)")) {
            ps.setLong(1, deleteFileId);
            ps.setLong(2, tableId);
            ps.setLong(3, beginSnapshot);
            ps.setLong(4, dataFileId);
            ps.setString(5, path);
            ps.setLong(6, deleteCount);
            ps.setLong(7, fileSizeBytes);
            ps.executeUpdate();
        }
    }

    /** Mark all active delete files for a table as deleted at the given snapshot. */
    public void markDeleteFilesDeleted(long tableId, long snapshotId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "UPDATE ducklake_delete_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
            ps.setLong(1, snapshotId);
            ps.setLong(2, tableId);
            ps.executeUpdate();
        }
    }

    // ---------------------------------------------------------------
    // Maintenance operations
    // ---------------------------------------------------------------

    /** Delete a snapshot record and its associated changes record. */
    public void deleteSnapshotRecord(long snapshotId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "DELETE FROM ducklake_snapshot_changes WHERE snapshot_id = ?")) {
            ps.setLong(1, snapshotId);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = getConnection().prepareStatement(
                "DELETE FROM ducklake_snapshot WHERE snapshot_id = ?")) {
            ps.setLong(1, snapshotId);
            ps.executeUpdate();
        }
    }

    /** Get all data files that have been logically deleted (end_snapshot IS NOT NULL). */
    public List<EndedFileInfo> getEndedDataFiles() throws SQLException {
        List<EndedFileInfo> result = new ArrayList<>();
        try (Statement s = getConnection().createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT data_file_id, table_id, begin_snapshot, end_snapshot, path, path_is_relative " +
                     "FROM ducklake_data_file WHERE end_snapshot IS NOT NULL")) {
            while (rs.next()) {
                result.add(new EndedFileInfo(
                        rs.getLong("data_file_id"),
                        rs.getLong("table_id"),
                        rs.getLong("begin_snapshot"),
                        rs.getLong("end_snapshot"),
                        rs.getString("path"),
                        getBooleanCompat(rs, "path_is_relative")));
            }
        }
        return result;
    }

    /** Get all delete files that have been logically deleted (end_snapshot IS NOT NULL). */
    public List<EndedFileInfo> getEndedDeleteFiles() throws SQLException {
        List<EndedFileInfo> result = new ArrayList<>();
        try (Statement s = getConnection().createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT delete_file_id, table_id, begin_snapshot, end_snapshot, path, path_is_relative " +
                     "FROM ducklake_delete_file WHERE end_snapshot IS NOT NULL")) {
            while (rs.next()) {
                result.add(new EndedFileInfo(
                        rs.getLong("delete_file_id"),
                        rs.getLong("table_id"),
                        rs.getLong("begin_snapshot"),
                        rs.getLong("end_snapshot"),
                        rs.getString("path"),
                        getBooleanCompat(rs, "path_is_relative")));
            }
        }
        return result;
    }

    /** Physically remove a data file record from the catalog. */
    public void removeDataFileRecord(long dataFileId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "DELETE FROM ducklake_data_file WHERE data_file_id = ?")) {
            ps.setLong(1, dataFileId);
            ps.executeUpdate();
        }
    }

    /** Physically remove a delete file record from the catalog. */
    public void removeDeleteFileRecord(long deleteFileId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "DELETE FROM ducklake_delete_file WHERE delete_file_id = ?")) {
            ps.setLong(1, deleteFileId);
            ps.executeUpdate();
        }
    }

    /** Remove column statistics for a specific data file. */
    public void removeColumnStatsForFile(long dataFileId, long tableId) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "DELETE FROM ducklake_file_column_stats WHERE data_file_id = ? AND table_id = ?")) {
            ps.setLong(1, dataFileId);
            ps.setLong(2, tableId);
            ps.executeUpdate();
        }
    }

    // ---------------------------------------------------------------
    // DDL operations (catalog plugin)
    // ---------------------------------------------------------------

    /** Look up a schema by name at the current snapshot. Returns null if not found. */
    public SchemaInfo getSchemaByName(String schemaName) throws SQLException {
        long snap = getCurrentSnapshotId();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT schema_id, schema_name, path, path_is_relative " +
                "FROM ducklake_schema " +
                "WHERE schema_name = ? AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setString(1, schemaName);
            ps.setLong(2, snap);
            ps.setLong(3, snap);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SchemaInfo(
                            rs.getLong("schema_id"),
                            rs.getString("schema_name"),
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"));
                }
            }
        }
        return null;
    }

    /**
     * Create a new schema in the catalog.
     * Creates a new snapshot and inserts the schema record.
     */
    public SchemaInfo createSchema(String schemaName) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;
            long schemaId = meta.nextCatalogId;

            createSnapshot(newSnap, meta.schemaVersion + 1, schemaId + 1, meta.nextFileId);
            insertSnapshotChanges(newSnap, "created_schema:\"" + schemaName + "\"",
                    "ducklake-spark", "Create schema " + schemaName);

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, " +
                    "schema_name, path, path_is_relative) VALUES (?, ?, ?, NULL, ?, ?, 1)")) {
                ps.setLong(1, schemaId);
                ps.setString(2, java.util.UUID.randomUUID().toString());
                ps.setLong(3, newSnap);
                ps.setString(4, schemaName);
                ps.setString(5, schemaName + "/");
                ps.executeUpdate();
            }

            commitTransaction();
            return new SchemaInfo(schemaId, schemaName, schemaName + "/", true);
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Drop a schema by marking it as deleted at a new snapshot.
     */
    public void dropSchema(long schemaId) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "dropped_schema:" + schemaId,
                    "ducklake-spark", "Drop schema");

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE ducklake_schema SET end_snapshot = ? WHERE schema_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, schemaId);
                ps.executeUpdate();
            }

            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Create a new table with columns. Creates a snapshot and inserts table + column records.
     */
    public TableInfo createTableEntry(long schemaId, String tableName,
                                      String[] colNames, String[] colTypes) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            long tableId = meta.nextCatalogId;
            long nextCatalogId = tableId + 1 + colNames.length;

            createSnapshot(newSnap, meta.schemaVersion + 1, nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "created_table:" + tableId,
                    "ducklake-spark", "Create table " + tableName);

            // Look up schema path
            String schemaPath = "";
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT path FROM ducklake_schema WHERE schema_id = ?")) {
                ps.setLong(1, schemaId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        schemaPath = rs.getString("path");
                        if (schemaPath == null) schemaPath = "";
                    }
                }
            }
            String tablePath = schemaPath;
            if (!tablePath.isEmpty() && !tablePath.endsWith("/")) tablePath += "/";
            tablePath += tableName + "/";

            String tableUuid = java.util.UUID.randomUUID().toString();

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO ducklake_table (table_id, table_uuid, begin_snapshot, end_snapshot, " +
                    "schema_id, table_name, path, path_is_relative) VALUES (?, ?, ?, NULL, ?, ?, ?, 1)")) {
                ps.setLong(1, tableId);
                ps.setString(2, tableUuid);
                ps.setLong(3, newSnap);
                ps.setLong(4, schemaId);
                ps.setString(5, tableName);
                ps.setString(6, tablePath);
                ps.executeUpdate();
            }

            long colId = tableId + 1;
            for (int i = 0; i < colNames.length; i++) {
                try (PreparedStatement ps = getConnection().prepareStatement(
                        "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, " +
                        "column_order, column_name, column_type, initial_default, default_value, " +
                        "nulls_allowed, parent_column, default_value_type, default_value_dialect) " +
                        "VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, NULL, 1, NULL, NULL, NULL)")) {
                    ps.setLong(1, colId);
                    ps.setLong(2, newSnap);
                    ps.setLong(3, tableId);
                    ps.setInt(4, i);
                    ps.setString(5, colNames[i]);
                    ps.setString(6, colTypes[i]);
                    ps.executeUpdate();
                }
                colId++;
            }

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) " +
                    "VALUES (?, 0, 0, 0)")) {
                ps.setLong(1, tableId);
                ps.executeUpdate();
            }

            commitTransaction();
            return new TableInfo(tableId, tableUuid, tableName, tablePath, true);
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Drop a table by marking it and its columns/data files as deleted.
     */
    public boolean dropTableEntry(long tableId) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "dropped_table:" + tableId,
                    "ducklake-spark", "Drop table");

            int updated;
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE ducklake_table SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                updated = ps.executeUpdate();
            }

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                ps.executeUpdate();
            }

            markDataFilesDeleted(tableId, newSnap);
            markDeleteFilesDeleted(tableId, newSnap);

            commitTransaction();
            return updated > 0;
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    // ---------------------------------------------------------------
    // ALTER TABLE operations
    // ---------------------------------------------------------------

    /**
     * Add a column to a table. Creates a new snapshot and inserts a column record.
     *
     * @param tableId    the table to alter
     * @param columnName new column name
     * @param columnType DuckDB type string (e.g. "INTEGER", "VARCHAR")
     * @param nullable   whether the column allows nulls
     * @return the column_id assigned to the new column
     */
    public long addColumn(long tableId, String columnName, String columnType, boolean nullable) throws SQLException {
        return addColumnWithDefault(tableId, columnName, columnType, nullable, null, null);
    }

    /**
     * Add a column with default values to a table.
     *
     * @param tableId The table to add the column to
     * @param columnName The name of the new column
     * @param columnType The DuckDB type of the new column
     * @param nullable Whether the column allows NULL values
     * @param initialDefault Default value for existing rows when column is added (for schema evolution)
     * @param writeDefault Default value for new inserts when column value is omitted
     * @return The column ID of the newly added column
     */
    public long addColumnWithDefault(long tableId, String columnName, String columnType, boolean nullable,
                                   String initialDefault, String writeDefault) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;
            long columnId = meta.nextCatalogId;

            createSnapshot(newSnap, meta.schemaVersion + 1, columnId + 1, meta.nextFileId);
            insertSnapshotChanges(newSnap, "added_column:\"" + columnName + "\"" +
                    (initialDefault != null || writeDefault != null ? " with defaults" : ""),
                    "ducklake-spark", "Add column " + columnName);

            // Determine column_order: max existing order + 1
            int colOrder = 0;
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT MAX(column_order) FROM ducklake_column " +
                    "WHERE table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, tableId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getObject(1) != null) {
                        colOrder = rs.getInt(1) + 1;
                    }
                }
            }

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, " +
                    "column_order, column_name, column_type, initial_default, default_value, " +
                    "nulls_allowed, parent_column, default_value_type, default_value_dialect) " +
                    "VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)")) {
                ps.setLong(1, columnId);
                ps.setLong(2, newSnap);
                ps.setLong(3, tableId);
                ps.setInt(4, colOrder);
                ps.setString(5, columnName);
                ps.setString(6, columnType);
                ps.setString(7, initialDefault);
                ps.setString(8, writeDefault);
                ps.setInt(9, nullable ? 1 : 0);
                ps.executeUpdate();
            }

            commitTransaction();
            return columnId;
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Drop a column from a table by setting its end_snapshot.
     *
     * @param tableId    the table to alter
     * @param columnName column to drop
     * @throws SQLException if the column is not found or DB error
     */
    public void dropColumn(long tableId, String columnName) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "dropped_column:\"" + columnName + "\"",
                    "ducklake-spark", "Drop column " + columnName);

            int updated;
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? " +
                    "WHERE table_id = ? AND column_name = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                ps.setString(3, columnName);
                updated = ps.executeUpdate();
            }

            if (updated == 0) {
                throw new SQLException("Column '" + columnName + "' not found in table " + tableId);
            }

            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Rename a column. Ends the old column record and inserts a new one with the same
     * column_id but a new name -- this preserves field-id-based matching for schema evolution.
     *
     * @param tableId the table to alter
     * @param oldName current column name
     * @param newName new column name
     * @throws SQLException if the column is not found or DB error
     */
    public void renameColumn(long tableId, String oldName, String newName) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            // Look up existing column metadata
            long columnId;
            String colType;
            int colOrder;
            boolean nullable;
            String initialDefault;
            String defaultValue;
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT column_id, column_type, column_order, nulls_allowed, initial_default, default_value " +
                    "FROM ducklake_column WHERE table_id = ? AND column_name = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, tableId);
                ps.setString(2, oldName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Column '" + oldName + "' not found in table " + tableId);
                    }
                    columnId = rs.getLong("column_id");
                    colType = rs.getString("column_type");
                    colOrder = rs.getInt("column_order");
                    nullable = getBooleanCompat(rs, "nulls_allowed");
                    initialDefault = rs.getString("initial_default");
                    defaultValue = rs.getString("default_value");
                }
            }

            createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "renamed_column:\"" + oldName + "\"->\"" + newName + "\"",
                    "ducklake-spark", "Rename column " + oldName + " to " + newName);

            // End old column record
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? " +
                    "WHERE column_id = ? AND table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, columnId);
                ps.setLong(3, tableId);
                ps.executeUpdate();
            }

            // Insert new record with same column_id but new name
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, " +
                    "column_order, column_name, column_type, initial_default, default_value, " +
                    "nulls_allowed, parent_column, default_value_type, default_value_dialect) " +
                    "VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)")) {
                ps.setLong(1, columnId);
                ps.setLong(2, newSnap);
                ps.setLong(3, tableId);
                ps.setInt(4, colOrder);
                ps.setString(5, newName);
                ps.setString(6, colType);
                if (initialDefault != null) {
                    ps.setString(7, initialDefault);
                } else {
                    ps.setNull(7, java.sql.Types.VARCHAR);
                }
                if (defaultValue != null) {
                    ps.setString(8, defaultValue);
                } else {
                    ps.setNull(8, java.sql.Types.VARCHAR);
                }
                ps.setInt(9, nullable ? 1 : 0);
                ps.executeUpdate();
            }

            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Update the type of a column. Ends the old column record and inserts a new one
     * with the same column_id but a new type.
     *
     * @param tableId    the table to alter
     * @param columnName column to update
     * @param newType    new DuckDB type string
     * @throws SQLException if the column is not found or DB error
     */
    public void updateColumnType(long tableId, String columnName, String newType) throws SQLException {
        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            // Look up existing column metadata
            long columnId;
            int colOrder;
            boolean nullable;
            String initialDefault;
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "SELECT column_id, column_order, nulls_allowed, initial_default " +
                    "FROM ducklake_column WHERE table_id = ? AND column_name = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, tableId);
                ps.setString(2, columnName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Column '" + columnName + "' not found in table " + tableId);
                    }
                    columnId = rs.getLong("column_id");
                    colOrder = rs.getInt("column_order");
                    nullable = getBooleanCompat(rs, "nulls_allowed");
                    initialDefault = rs.getString("initial_default");
                }
            }

            createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "updated_column_type:\"" + columnName + "\"->\"" + newType + "\"",
                    "ducklake-spark", "Update column type " + columnName + " to " + newType);

            // End old column record
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? " +
                    "WHERE column_id = ? AND table_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, columnId);
                ps.setLong(3, tableId);
                ps.executeUpdate();
            }

            // Insert new record with same column_id but new type
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, " +
                    "column_order, column_name, column_type, initial_default, default_value, " +
                    "nulls_allowed, parent_column, default_value_type, default_value_dialect) " +
                    "VALUES (?, ?, NULL, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL)")) {
                ps.setLong(1, columnId);
                ps.setLong(2, newSnap);
                ps.setLong(3, tableId);
                ps.setInt(4, colOrder);
                ps.setString(5, columnName);
                ps.setString(6, newType);
                if (initialDefault != null) {
                    ps.setString(7, initialDefault);
                } else {
                    ps.setNull(7, java.sql.Types.VARCHAR);
                }
                ps.setInt(8, nullable ? 1 : 0);
                ps.executeUpdate();
            }

            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    // ---------------------------------------------------------------
    // Constraints
    // ---------------------------------------------------------------

    /**
     * Get all constraints for a table at a specific snapshot.
     *
     * @param tableId The table ID
     * @param snapshotId The snapshot ID (use -1 for current)
     * @return List of table constraints
     * @throws SQLException on database error
     */
    public List<ConstraintInfo> getConstraints(long tableId, long snapshotId) throws SQLException {
        List<ConstraintInfo> result = new ArrayList<>();
        long actualSnapshot = snapshotId == -1 ? getCurrentSnapshotId() : snapshotId;

        // Check if ducklake_table_constraint table exists, create if not
        ensureConstraintTableExists();

        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT constraint_type, column_names, constraint_name " +
                "FROM ducklake_table_constraint " +
                "WHERE table_id = ? AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY constraint_name")) {
            ps.setLong(1, tableId);
            ps.setLong(2, actualSnapshot);
            ps.setLong(3, actualSnapshot);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String constraintType = rs.getString("constraint_type");
                    String columnNamesStr = rs.getString("column_names");
                    String constraintName = rs.getString("constraint_name");

                    // Parse column names (comma-separated)
                    String[] columnNames = columnNamesStr != null ? columnNamesStr.split(",") : new String[0];
                    for (int i = 0; i < columnNames.length; i++) {
                        columnNames[i] = columnNames[i].trim();
                    }

                    result.add(new ConstraintInfo(constraintName, constraintType, columnNames));
                }
            }
        }
        return result;
    }

    /**
     * Get all constraints for a table (at current snapshot).
     *
     * @param tableId The table ID
     * @return List of table constraints
     * @throws SQLException on database error
     */
    public List<ConstraintInfo> getConstraints(long tableId) throws SQLException {
        return getConstraints(tableId, -1);
    }

    /**
     * Add a constraint to a table.
     *
     * @param tableId The table ID
     * @param constraintName The name of the constraint
     * @param constraintType The type of constraint (NOT NULL, UNIQUE, CHECK)
     * @param columnNames The columns the constraint applies to
     * @throws SQLException on database error
     */
    public void addConstraint(long tableId, String constraintName, String constraintType, String[] columnNames) throws SQLException {
        ensureConstraintTableExists();

        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "added_constraint:\"" + constraintName + "\"",
                    "ducklake-spark", "Add constraint " + constraintName);

            // Join column names with comma
            String columnNamesStr = String.join(",", columnNames);

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "INSERT INTO ducklake_table_constraint (table_id, begin_snapshot, end_snapshot, " +
                    "constraint_name, constraint_type, column_names) " +
                    "VALUES (?, ?, NULL, ?, ?, ?)")) {
                ps.setLong(1, tableId);
                ps.setLong(2, newSnap);
                ps.setString(3, constraintName);
                ps.setString(4, constraintType);
                ps.setString(5, columnNamesStr);
                ps.executeUpdate();
            }

            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Drop a constraint from a table.
     *
     * @param tableId The table ID
     * @param constraintName The name of the constraint to drop
     * @throws SQLException on database error
     */
    public void dropConstraint(long tableId, String constraintName) throws SQLException {
        ensureConstraintTableExists();

        beginTransaction();
        try {
            long currentSnap = getCurrentSnapshotId();
            CatalogState meta = getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);
            insertSnapshotChanges(newSnap, "dropped_constraint:\"" + constraintName + "\"",
                    "ducklake-spark", "Drop constraint " + constraintName);

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "UPDATE ducklake_table_constraint SET end_snapshot = ? " +
                    "WHERE table_id = ? AND constraint_name = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                ps.setString(3, constraintName);
                int updated = ps.executeUpdate();
                if (updated == 0) {
                    throw new SQLException("Constraint not found: " + constraintName);
                }
            }

            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Ensure the ducklake_table_constraint table exists.
     */
    private void ensureConstraintTableExists() throws SQLException {
        try (Statement st = getConnection().createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS ducklake_table_constraint(" +
                    "table_id BIGINT, " +
                    "begin_snapshot BIGINT, " +
                    "end_snapshot BIGINT, " +
                    "constraint_name VARCHAR, " +
                    "constraint_type VARCHAR, " +
                    "column_names VARCHAR)");
        }
    }

    // ---------------------------------------------------------------
    // Data classes
    // ---------------------------------------------------------------

    public static class SnapshotInfo {
        public final long snapshotId;
        public final String snapshotTime;
        public final String changes;

        public SnapshotInfo(long snapshotId, String snapshotTime, String changes) {
            this.snapshotId = snapshotId;
            this.snapshotTime = snapshotTime;
            this.changes = changes;
        }
    }

    public static class SchemaInfo {
        public final long schemaId;
        public final String name;
        public final String path;
        public final boolean pathIsRelative;

        public SchemaInfo(long schemaId, String name, String path, boolean pathIsRelative) {
            this.schemaId = schemaId;
            this.name = name;
            this.path = path;
            this.pathIsRelative = pathIsRelative;
        }
    }

    public static class TableInfo {
        public final long tableId;
        public final String uuid;
        public final String name;
        public final String path;
        public final boolean pathIsRelative;

        public TableInfo(long tableId, String uuid, String name, String path, boolean pathIsRelative) {
            this.tableId = tableId;
            this.uuid = uuid;
            this.name = name;
            this.path = path;
            this.pathIsRelative = pathIsRelative;
        }
    }

    public static class ColumnInfo {
        public final long columnId;
        public final String name;
        public final String type;
        public final int order;
        public final String initialDefault;
        public final String defaultValue;
        public final boolean nullable;
        public final long parentColumn;

        public ColumnInfo(long columnId, String name, String type, int order,
                          String initialDefault, String defaultValue,
                          boolean nullable, long parentColumn) {
            this.columnId = columnId;
            this.name = name;
            this.type = type;
            this.order = order;
            this.initialDefault = initialDefault;
            this.defaultValue = defaultValue;
            this.nullable = nullable;
            this.parentColumn = parentColumn;
        }
    }

    public static class ConstraintInfo {
        public final String name;
        public final String type;
        public final String[] columnNames;

        public ConstraintInfo(String name, String type, String[] columnNames) {
            this.name = name;
            this.type = type;
            this.columnNames = columnNames;
        }
    }

    public static class DataFileInfo {
        public final long dataFileId;
        public final String path;
        public final boolean pathIsRelative;
        public final String format;
        public final long recordCount;
        public final long fileSizeBytes;
        public final long mappingId;
        public final long partitionId;
        public final long beginSnapshot;
        public final long rowIdStart;

        public DataFileInfo(long dataFileId, String path, boolean pathIsRelative,
                            String format, long recordCount, long fileSizeBytes,
                            long mappingId, long partitionId) {
            this(dataFileId, path, pathIsRelative, format, recordCount, fileSizeBytes, mappingId, partitionId, -1L, 0L);
        }

        public DataFileInfo(long dataFileId, String path, boolean pathIsRelative,
                            String format, long recordCount, long fileSizeBytes,
                            long mappingId, long partitionId, long beginSnapshot) {
            this(dataFileId, path, pathIsRelative, format, recordCount, fileSizeBytes, mappingId, partitionId, beginSnapshot, 0L);
        }

        public DataFileInfo(long dataFileId, String path, boolean pathIsRelative,
                            String format, long recordCount, long fileSizeBytes,
                            long mappingId, long partitionId, long beginSnapshot, long rowIdStart) {
            this.dataFileId = dataFileId;
            this.path = path;
            this.pathIsRelative = pathIsRelative;
            this.format = format;
            this.recordCount = recordCount;
            this.fileSizeBytes = fileSizeBytes;
            this.mappingId = mappingId;
            this.partitionId = partitionId;
            this.beginSnapshot = beginSnapshot;
            this.rowIdStart = rowIdStart;
        }
    }

    public static class DeleteFileInfo {
        public final long deleteFileId;
        public final long dataFileId;
        public final String path;
        public final boolean pathIsRelative;
        public final String format;
        public final long deleteCount;
        public final long beginSnapshot;

        public DeleteFileInfo(long deleteFileId, String path, boolean pathIsRelative,
                              String format, long deleteCount) {
            this(deleteFileId, -1, path, pathIsRelative, format, deleteCount, -1L);
        }

        public DeleteFileInfo(long deleteFileId, String path, boolean pathIsRelative,
                              String format, long deleteCount, long beginSnapshot) {
            this(deleteFileId, -1, path, pathIsRelative, format, deleteCount, beginSnapshot);
        }

        public DeleteFileInfo(long deleteFileId, long dataFileId, String path, boolean pathIsRelative,
                              String format, long deleteCount, long beginSnapshot) {
            this.deleteFileId = deleteFileId;
            this.dataFileId = dataFileId;
            this.path = path;
            this.pathIsRelative = pathIsRelative;
            this.format = format;
            this.deleteCount = deleteCount;
            this.beginSnapshot = beginSnapshot;
        }
    }

    public static class FileColumnStats {
        public final long columnId;
        public final String minValue;
        public final String maxValue;
        public final long nullCount;
        public final long valueCount;

        public FileColumnStats(long columnId, String minValue, String maxValue,
                               long nullCount, long valueCount) {
            this.columnId = columnId;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.nullCount = nullCount;
            this.valueCount = valueCount;
        }
    }

    public static class CatalogState {
        public final long schemaVersion;
        public final long nextCatalogId;
        public final long nextFileId;

        public CatalogState(long schemaVersion, long nextCatalogId, long nextFileId) {
            this.schemaVersion = schemaVersion;
            this.nextCatalogId = nextCatalogId;
            this.nextFileId = nextFileId;
        }
    }

    public static class TableStats {
        public final long recordCount;
        public final long nextRowId;
        public final long fileSizeBytes;

        public TableStats(long recordCount, long nextRowId, long fileSizeBytes) {
            this.recordCount = recordCount;
            this.nextRowId = nextRowId;
            this.fileSizeBytes = fileSizeBytes;
        }
    }

    public static class EndedFileInfo {
        public final long fileId;
        public final long tableId;
        public final long beginSnapshot;
        public final long endSnapshot;
        public final String path;
        public final boolean pathIsRelative;

        public EndedFileInfo(long fileId, long tableId, long beginSnapshot, long endSnapshot,
                             String path, boolean pathIsRelative) {
            this.fileId = fileId;
            this.tableId = tableId;
            this.beginSnapshot = beginSnapshot;
            this.endSnapshot = endSnapshot;
            this.path = path;
            this.pathIsRelative = pathIsRelative;
        }
    }

    public static class PartitionInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public final long columnId;
        public final String columnName;
        public final String partitionExpression;  // null for identity partitions
        public final int partitionIndex;

        public PartitionInfo(long columnId, String columnName, String partitionExpression, int partitionIndex) {
            this.columnId = columnId;
            this.columnName = columnName;
            this.partitionExpression = partitionExpression;
            this.partitionIndex = partitionIndex;
        }

        public boolean isIdentityPartition() {
            return partitionExpression == null;
        }
    }

    public static class PartitionFilter {
        public final int partitionIndex;
        public final String operator;  // "=", "IN", "<", ">", "<=", ">="
        public final Object value;     // Single value or List for IN operator

        public PartitionFilter(int partitionIndex, String operator, Object value) {
            this.partitionIndex = partitionIndex;
            this.operator = operator;
            this.value = value;
        }
    }

    /** Get ALL active delete files for a table at a specific snapshot (bulk query, no N+1). */
    public Map<Long, List<DeleteFileInfo>> getAllDeleteFilesForTable(long tableId, long snapshotId) throws SQLException {
        Map<Long, List<DeleteFileInfo>> result = new HashMap<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT delete_file_id, data_file_id, path, path_is_relative, format, delete_count " +
                "FROM ducklake_delete_file " +
                "WHERE table_id = ? AND begin_snapshot <= ? " +
                "AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setLong(1, tableId);
            ps.setLong(2, snapshotId);
            ps.setLong(3, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long dataFileId = rs.getLong("data_file_id");
                    result.computeIfAbsent(dataFileId, k -> new ArrayList<>()).add(
                        new DeleteFileInfo(
                            rs.getLong("delete_file_id"),
                            dataFileId,
                            rs.getString("path"),
                            getBooleanCompat(rs, "path_is_relative"),
                            rs.getString("format"),
                            rs.getLong("delete_count"),
                            -1L));
                }
            }
        }
        return result;
    }

    /** Get ALL column stats for all active files of a table (bulk query, no N+1). */
    public Map<Long, List<FileColumnStats>> getAllFileColumnStatsForTable(long tableId, long snapshotId) throws SQLException {
        Map<Long, List<FileColumnStats>> result = new HashMap<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT data_file_id, column_id, min_value, max_value, null_count, value_count " +
                "FROM ducklake_file_column_stats " +
                "WHERE table_id = ?")) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long fileId = rs.getLong("data_file_id");
                    result.computeIfAbsent(fileId, k -> new ArrayList<>()).add(
                        new FileColumnStats(
                            rs.getLong("column_id"),
                            rs.getString("min_value"),
                            rs.getString("max_value"),
                            rs.getLong("null_count"),
                            rs.getLong("value_count")));
                }
            }
        }
        return result;
    }

    /** Get ALL name mappings referenced by active files (bulk query, no N+1). */
    public Map<Long, Map<Long, String>> getAllNameMappingsForTable(long tableId, long snapshotId) throws SQLException {
        Map<Long, Map<Long, String>> result = new HashMap<>();
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT DISTINCT df.mapping_id, nm.target_field_id, nm.source_name " +
                "FROM ducklake_data_file df " +
                "JOIN ducklake_name_mapping nm ON df.mapping_id = nm.mapping_id " +
                "WHERE df.table_id = ? AND df.begin_snapshot <= ? " +
                "AND (df.end_snapshot IS NULL OR df.end_snapshot > ?) " +
                "AND df.mapping_id IS NOT NULL")) {
            ps.setLong(1, tableId);
            ps.setLong(2, snapshotId);
            ps.setLong(3, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long mappingId = rs.getLong("mapping_id");
                    result.computeIfAbsent(mappingId, k -> new HashMap<>())
                        .put(rs.getLong("target_field_id"), rs.getString("source_name"));
                }
            }
        }
        return result;
    }

}
