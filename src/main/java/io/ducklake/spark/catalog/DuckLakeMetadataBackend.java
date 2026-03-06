package io.ducklake.spark.catalog;

import java.sql.*;
import java.util.*;

/**
 * Reads DuckLake catalog metadata from a SQL database (SQLite or PostgreSQL).
 * This is the core abstraction over the DuckLake metadata schema.
 */
public class DuckLakeMetadataBackend implements AutoCloseable {
    private final String jdbcUrl;
    private final String dataPath;
    private Connection connection;

    public DuckLakeMetadataBackend(String catalogPath, String dataPath) {
        if (catalogPath.startsWith("postgresql://") || catalogPath.startsWith("jdbc:postgresql:")) {
            this.jdbcUrl = catalogPath.startsWith("jdbc:") ? catalogPath : "jdbc:" + catalogPath;
        } else {
            // SQLite (default)
            this.jdbcUrl = "jdbc:sqlite:" + catalogPath;
        }
        this.dataPath = dataPath;
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcUrl);
        }
        return connection;
    }

    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
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
                "SELECT snapshot_id, snapshot_time, snapshot_changes " +
                "FROM ducklake_snapshot WHERE snapshot_id = ?")) {
            ps.setLong(1, version);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SnapshotInfo(
                            rs.getLong("snapshot_id"),
                            rs.getString("snapshot_time"),
                            rs.getString("snapshot_changes"));
                }
            }
        }
        return null;
    }

    /** Get the latest snapshot at or before the given timestamp. Returns null if none found. */
    public SnapshotInfo getSnapshotAtTime(String timestamp) throws SQLException {
        try (PreparedStatement ps = getConnection().prepareStatement(
                "SELECT snapshot_id, snapshot_time, snapshot_changes " +
                "FROM ducklake_snapshot " +
                "WHERE snapshot_time <= ? ORDER BY snapshot_id DESC LIMIT 1")) {
            ps.setString(1, timestamp);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SnapshotInfo(
                            rs.getLong("snapshot_id"),
                            rs.getString("snapshot_time"),
                            rs.getString("snapshot_changes"));
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
                     "SELECT snapshot_id, snapshot_time, snapshot_changes " +
                     "FROM ducklake_snapshot ORDER BY snapshot_id")) {
            while (rs.next()) {
                result.add(new SnapshotInfo(
                        rs.getLong("snapshot_id"),
                        rs.getString("snapshot_time"),
                        rs.getString("snapshot_changes")));
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
                            rs.getInt("path_is_relative") == 1));
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
                            rs.getInt("path_is_relative") == 1));
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
                            rs.getInt("path_is_relative") == 1);
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
                            rs.getInt("path_is_relative") == 1);
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
                            rs.getInt("nulls_allowed") == 1,
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
                            rs.getInt("path_is_relative") == 1,
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
                            rs.getInt("path_is_relative") == 1,
                            rs.getString("format"),
                            rs.getLong("delete_count")));
                }
            }
        }
        return result;
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
    }

    /** Rollback the current transaction. */
    public void rollbackTransaction() throws SQLException {
        try {
            getConnection().rollback();
        } finally {
            getConnection().setAutoCommit(true);
        }
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
                "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES (?, datetime('now'), ?, ?, ?)")) {
            ps.setLong(1, snapshotId);
            ps.setLong(2, schemaVersion);
            ps.setLong(3, nextCatalogId);
            ps.setLong(4, nextFileId);
            ps.executeUpdate();
        }
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

    public static class DataFileInfo {
        public final long dataFileId;
        public final String path;
        public final boolean pathIsRelative;
        public final String format;
        public final long recordCount;
        public final long fileSizeBytes;
        public final long mappingId;
        public final long partitionId;

        public DataFileInfo(long dataFileId, String path, boolean pathIsRelative,
                            String format, long recordCount, long fileSizeBytes,
                            long mappingId, long partitionId) {
            this.dataFileId = dataFileId;
            this.path = path;
            this.pathIsRelative = pathIsRelative;
            this.format = format;
            this.recordCount = recordCount;
            this.fileSizeBytes = fileSizeBytes;
            this.mappingId = mappingId;
            this.partitionId = partitionId;
        }
    }

    public static class DeleteFileInfo {
        public final long deleteFileId;
        public final String path;
        public final boolean pathIsRelative;
        public final String format;
        public final long deleteCount;

        public DeleteFileInfo(long deleteFileId, String path, boolean pathIsRelative,
                              String format, long deleteCount) {
            this.deleteFileId = deleteFileId;
            this.path = path;
            this.pathIsRelative = pathIsRelative;
            this.format = format;
            this.deleteCount = deleteCount;
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
}
