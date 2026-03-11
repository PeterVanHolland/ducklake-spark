package io.ducklake.spark.catalog;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import java.sql.*;
import java.util.*;

/**
 * Manages views in the DuckLake catalog.
 * Views are stored as SQL text in the ducklake_view table.
 */
public class DuckLakeViewManager {

    private final DuckLakeMetadataBackend backend;

    public DuckLakeViewManager(DuckLakeMetadataBackend backend) {
        this.backend = backend;
    }

    /**
     * Create a view in the catalog.
     *
     * @param schemaId   Schema to create the view in
     * @param viewName   Name of the view
     * @param sql        SQL definition of the view
     * @param orReplace  If true, replace existing view with same name
     * @return the view_id
     */
    public long createView(long schemaId, String viewName, String sql, boolean orReplace) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureViewTable(conn);

        backend.beginTransaction();
        try {
            long currentSnap = backend.getCurrentSnapshotId();
            CatalogState meta = backend.getSnapshotInfo(currentSnap);

            // Check for existing view
            Long existingViewId = getViewId(conn, viewName, schemaId, currentSnap);

            if (existingViewId != null && !orReplace) {
                throw new SQLException("View '" + viewName + "' already exists");
            }

            long viewId = meta.nextCatalogId;
            long newSnap = currentSnap + 1;
            long newNextCatalogId = viewId + 1;

            backend.createSnapshot(newSnap, meta.schemaVersion + 1, newNextCatalogId, meta.nextFileId);

            List<String> changes = new ArrayList<>();

            // End existing view if replacing
            if (existingViewId != null) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE ducklake_view SET end_snapshot = ? " +
                        "WHERE view_id = ? AND end_snapshot IS NULL")) {
                    ps.setLong(1, newSnap);
                    ps.setLong(2, existingViewId);
                    ps.executeUpdate();
                }
                changes.add("dropped_view:" + existingViewId);
            }

            // Insert new view record
            String viewUuid = UUID.randomUUID().toString();
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ducklake_view " +
                    "(view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, " +
                    "view_name, dialect, sql, column_aliases) " +
                    "VALUES (?, ?, ?, NULL, ?, ?, 'duckdb', ?, '')")) {
                ps.setLong(1, viewId);
                ps.setString(2, viewUuid);
                ps.setLong(3, newSnap);
                ps.setLong(4, schemaId);
                ps.setString(5, viewName);
                ps.setString(6, sql);
                ps.executeUpdate();
            }

            changes.add("created_view:\"" + viewName + "\"");
            backend.insertSnapshotChanges(newSnap, String.join(",", changes),
                    "ducklake-spark", "Create view " + viewName);

            backend.commitTransaction();
            return viewId;
        } catch (Exception e) {
            backend.rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Drop a view from the catalog.
     */
    public void dropView(long schemaId, String viewName) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureViewTable(conn);

        backend.beginTransaction();
        try {
            long currentSnap = backend.getCurrentSnapshotId();
            CatalogState meta = backend.getSnapshotInfo(currentSnap);

            Long viewId = getViewId(conn, viewName, schemaId, currentSnap);
            if (viewId == null) {
                throw new SQLException("View '" + viewName + "' not found");
            }

            long newSnap = currentSnap + 1;
            backend.createSnapshot(newSnap, meta.schemaVersion + 1, meta.nextCatalogId, meta.nextFileId);

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ducklake_view SET end_snapshot = ? " +
                    "WHERE view_id = ? AND end_snapshot IS NULL")) {
                ps.setLong(1, newSnap);
                ps.setLong(2, viewId);
                ps.executeUpdate();
            }

            backend.insertSnapshotChanges(newSnap, "dropped_view:\"" + viewName + "\"",
                    "ducklake-spark", "Drop view " + viewName);

            backend.commitTransaction();
        } catch (Exception e) {
            backend.rollbackTransaction();
            throw e instanceof SQLException ? (SQLException) e : new SQLException(e);
        }
    }

    /**
     * Get a view's SQL definition.
     */
    public ViewInfo getView(long schemaId, String viewName) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureViewTable(conn);
        long snap = backend.getCurrentSnapshotId();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT view_id, view_uuid, view_name, sql, column_aliases " +
                "FROM ducklake_view " +
                "WHERE schema_id = ? AND view_name = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setLong(1, schemaId);
            ps.setString(2, viewName);
            ps.setLong(3, snap);
            ps.setLong(4, snap);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new ViewInfo(
                            rs.getLong("view_id"),
                            rs.getString("view_uuid"),
                            rs.getString("view_name"),
                            rs.getString("sql"),
                            rs.getString("column_aliases"));
                }
            }
        }
        return null;
    }

    /**
     * List all views in a schema.
     */
    public List<ViewInfo> listViews(long schemaId) throws SQLException {
        Connection conn = backend.getConnectionForInlining();
        ensureViewTable(conn);
        long snap = backend.getCurrentSnapshotId();

        List<ViewInfo> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT view_id, view_uuid, view_name, sql, column_aliases " +
                "FROM ducklake_view " +
                "WHERE schema_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY view_name")) {
            ps.setLong(1, schemaId);
            ps.setLong(2, snap);
            ps.setLong(3, snap);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new ViewInfo(
                            rs.getLong("view_id"),
                            rs.getString("view_uuid"),
                            rs.getString("view_name"),
                            rs.getString("sql"),
                            rs.getString("column_aliases")));
                }
            }
        }
        return result;
    }

    // ---------------------------------------------------------------

    private Long getViewId(Connection conn, String viewName, long schemaId, long snapshotId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT view_id FROM ducklake_view " +
                "WHERE view_name = ? AND schema_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)")) {
            ps.setString(1, viewName);
            ps.setLong(2, schemaId);
            ps.setLong(3, snapshotId);
            ps.setLong(4, snapshotId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }

    private void ensureViewTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS ducklake_view (" +
                    "view_id BIGINT, view_uuid VARCHAR, " +
                    "begin_snapshot BIGINT, end_snapshot BIGINT, " +
                    "schema_id BIGINT, view_name VARCHAR, " +
                    "dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR)");
        }
    }

    // ---------------------------------------------------------------
    // Data classes
    // ---------------------------------------------------------------

    public static class ViewInfo {
        public final long viewId;
        public final String uuid;
        public final String name;
        public final String sql;
        public final String columnAliases;

        public ViewInfo(long viewId, String uuid, String name, String sql, String columnAliases) {
            this.viewId = viewId;
            this.uuid = uuid;
            this.name = name;
            this.sql = sql;
            this.columnAliases = columnAliases;
        }
    }
}
