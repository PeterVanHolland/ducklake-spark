package io.ducklake.spark.catalog;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import java.sql.*;
import java.util.*;

/**
 * Manages tags (key-value metadata) on tables and columns in the DuckLake catalog.
 * Tags are versioned via snapshots (begin_snapshot / end_snapshot).
 */
public class DuckLakeTagManager {

    private final DuckLakeMetadataBackend backend;

    public DuckLakeTagManager(DuckLakeMetadataBackend backend) {
        this.backend = backend;
    }

    // ---------------------------------------------------------------
    // Table tags
    // ---------------------------------------------------------------

    /**
     * Set a tag on a table. Overwrites any existing tag with the same key.
     */
    public void setTableTag(long tableId, String key, String value) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureTagTable(conn);

        backend.beginTransaction();
        try {
            long currentSnap = backend.getCurrentSnapshotId();
            CatalogState meta = backend.getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            backend.createSnapshot(newSnap, meta.schemaVersion, meta.nextCatalogId, meta.nextFileId);

            // End any existing tag with the same key
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ducklake_tag SET end_snapshot = ? " +
                    "WHERE object_id = ? AND key = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                ps.setString(3, key);
                ps.executeUpdate();
            }

            // Insert the new tag
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value) " +
                    "VALUES (?, ?, NULL, ?, ?)")) {
                ps.setLong(1, tableId);
                ps.setLong(2, newSnap);
                ps.setString(3, key);
                ps.setString(4, value);
                ps.executeUpdate();
            }

            backend.insertSnapshotChanges(newSnap, "set_table_tag:\"" + key + "\"",
                    "ducklake-spark", "Set table tag " + key);

            backend.commitTransaction();
        } catch (Exception e) {
            backend.rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Delete a tag from a table.
     */
    public void deleteTableTag(long tableId, String key) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureTagTable(conn);

        backend.beginTransaction();
        try {
            long currentSnap = backend.getCurrentSnapshotId();
            CatalogState meta = backend.getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            backend.createSnapshot(newSnap, meta.schemaVersion, meta.nextCatalogId, meta.nextFileId);

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ducklake_tag SET end_snapshot = ? " +
                    "WHERE object_id = ? AND key = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                ps.setString(3, key);
                int updated = ps.executeUpdate();
                if (updated == 0) {
                    throw new SQLException("Tag '" + key + "' not found on table " + tableId);
                }
            }

            backend.insertSnapshotChanges(newSnap, "delete_table_tag:\"" + key + "\"",
                    "ducklake-spark", "Delete table tag " + key);

            backend.commitTransaction();
        } catch (Exception e) {
            backend.rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Get all tags for a table at the current snapshot.
     */
    public Map<String, String> getTableTags(long tableId) throws SQLException {
        return getTableTags(tableId, backend.getCurrentSnapshotId());
    }

    /**
     * Get all tags for a table at a specific snapshot.
     */
    public Map<String, String> getTableTags(long tableId, long snapshotId) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureTagTable(conn);

        Map<String, String> tags = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT key, value FROM ducklake_tag " +
                "WHERE object_id = ? AND begin_snapshot <= ? " +
                "AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY key")) {
            ps.setLong(1, tableId);
            ps.setLong(2, snapshotId);
            ps.setLong(3, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tags.put(rs.getString("key"), rs.getString("value"));
                }
            }
        }
        return tags;
    }

    // ---------------------------------------------------------------
    // Column tags
    // ---------------------------------------------------------------

    /**
     * Set a tag on a column.
     */
    public void setColumnTag(long tableId, long columnId, String key, String value) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureColumnTagTable(conn);

        backend.beginTransaction();
        try {
            long currentSnap = backend.getCurrentSnapshotId();
            CatalogState meta = backend.getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            backend.createSnapshot(newSnap, meta.schemaVersion, meta.nextCatalogId, meta.nextFileId);

            // End existing tag
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ducklake_column_tag SET end_snapshot = ? " +
                    "WHERE table_id = ? AND column_id = ? AND key = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                ps.setLong(3, columnId);
                ps.setString(4, key);
                ps.executeUpdate();
            }

            // Insert new tag
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_column_tag " +
                    "(table_id, column_id, begin_snapshot, end_snapshot, key, value) " +
                    "VALUES (?, ?, ?, NULL, ?, ?)")) {
                ps.setLong(1, tableId);
                ps.setLong(2, columnId);
                ps.setLong(3, newSnap);
                ps.setString(4, key);
                ps.setString(5, value);
                ps.executeUpdate();
            }

            backend.insertSnapshotChanges(newSnap, "set_column_tag:\"" + key + "\"",
                    "ducklake-spark", "Set column tag " + key);

            backend.commitTransaction();
        } catch (Exception e) {
            backend.rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Delete a tag from a column.
     */
    public void deleteColumnTag(long tableId, long columnId, String key) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureColumnTagTable(conn);

        backend.beginTransaction();
        try {
            long currentSnap = backend.getCurrentSnapshotId();
            CatalogState meta = backend.getSnapshotInfo(currentSnap);
            long newSnap = currentSnap + 1;

            backend.createSnapshot(newSnap, meta.schemaVersion, meta.nextCatalogId, meta.nextFileId);

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ducklake_column_tag SET end_snapshot = ? " +
                    "WHERE table_id = ? AND column_id = ? AND key = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, tableId);
                ps.setLong(3, columnId);
                ps.setString(4, key);
                int updated = ps.executeUpdate();
                if (updated == 0) {
                    throw new SQLException("Tag '" + key + "' not found on column");
                }
            }

            backend.insertSnapshotChanges(newSnap, "delete_column_tag:\"" + key + "\"",
                    "ducklake-spark", "Delete column tag " + key);

            backend.commitTransaction();
        } catch (Exception e) {
            backend.rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Get all tags for a column.
     */
    public Map<String, String> getColumnTags(long tableId, long columnId) throws SQLException {
        return getColumnTags(tableId, columnId, backend.getCurrentSnapshotId());
    }

    /**
     * Get all tags for a column at a specific snapshot.
     */
    public Map<String, String> getColumnTags(long tableId, long columnId, long snapshotId) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureColumnTagTable(conn);

        Map<String, String> tags = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT key, value FROM ducklake_column_tag " +
                "WHERE table_id = ? AND column_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY key")) {
            ps.setLong(1, tableId);
            ps.setLong(2, columnId);
            ps.setLong(3, snapshotId);
            ps.setLong(4, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tags.put(rs.getString("key"), rs.getString("value"));
                }
            }
        }
        return tags;
    }

    // ---------------------------------------------------------------
    // Comments (stored as special tags with key="comment")
    // ---------------------------------------------------------------

    public void setTableComment(long tableId, String comment) throws SQLException {
        setTableTag(tableId, "comment", comment);
    }

    public String getTableComment(long tableId) throws SQLException {
        Map<String, String> tags = getTableTags(tableId);
        return tags.get("comment");
    }

    public void setColumnComment(long tableId, long columnId, String comment) throws SQLException {
        setColumnTag(tableId, columnId, "comment", comment);
    }

    public String getColumnComment(long tableId, long columnId) throws SQLException {
        Map<String, String> tags = getColumnTags(tableId, columnId);
        return tags.get("comment");
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private void ensureTagTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS ducklake_tag (" +
                    "object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, " +
                    "key VARCHAR, value VARCHAR)");
        }
    }

    private void ensureColumnTagTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS ducklake_column_tag (" +
                    "table_id BIGINT, column_id BIGINT, " +
                    "begin_snapshot BIGINT, end_snapshot BIGINT, " +
                    "key VARCHAR, value VARCHAR)");
        }
    }
}
