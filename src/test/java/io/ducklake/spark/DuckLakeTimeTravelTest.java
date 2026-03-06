package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for DuckLake time travel: reading historical snapshots
 * by version (snapshot_id) or by timestamp (snapshot_time).
 *
 * Sets up a SQLite catalog with 3 snapshots, each adding one data file:
 *   Snapshot 1 (2026-01-01): file1 (2 rows)
 *   Snapshot 2 (2026-01-02): file2 (1 row)
 *   Snapshot 3 (2026-01-03): file3 (1 row)
 *
 * At snapshot 1: 1 file  (2 rows)
 * At snapshot 2: 2 files (3 rows)
 * At snapshot 3: 3 files (4 rows)
 */
public class DuckLakeTimeTravelTest {

    private File catalogFile;
    private DuckLakeMetadataBackend backend;

    @Before
    public void setUp() throws Exception {
        // Create a temp SQLite file for the catalog
        catalogFile = File.createTempFile("ducklake_test_", ".db");
        catalogFile.deleteOnExit();

        // Set up the DuckLake schema and insert test data
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogFile.getAbsolutePath())) {
            try (Statement s = conn.createStatement()) {
                // --- DuckLake metadata tables ---
                s.execute("CREATE TABLE ducklake_metadata ("
                        + "key TEXT, value TEXT, scope TEXT)");
                s.execute("CREATE TABLE ducklake_snapshot ("
                        + "snapshot_id INTEGER PRIMARY KEY, "
                        + "snapshot_time TEXT, "
                        + "snapshot_changes TEXT)");
                s.execute("CREATE TABLE ducklake_schema ("
                        + "schema_id INTEGER PRIMARY KEY, "
                        + "schema_name TEXT, path TEXT, path_is_relative INTEGER, "
                        + "begin_snapshot INTEGER, end_snapshot INTEGER)");
                s.execute("CREATE TABLE ducklake_table ("
                        + "table_id INTEGER PRIMARY KEY, "
                        + "table_uuid TEXT, table_name TEXT, schema_id INTEGER, "
                        + "path TEXT, path_is_relative INTEGER, "
                        + "begin_snapshot INTEGER, end_snapshot INTEGER)");
                s.execute("CREATE TABLE ducklake_column ("
                        + "column_id INTEGER PRIMARY KEY, "
                        + "table_id INTEGER, column_name TEXT, column_type TEXT, "
                        + "column_order INTEGER, initial_default TEXT, default_value TEXT, "
                        + "nulls_allowed INTEGER, parent_column INTEGER, "
                        + "begin_snapshot INTEGER, end_snapshot INTEGER)");
                s.execute("CREATE TABLE ducklake_data_file ("
                        + "data_file_id INTEGER PRIMARY KEY, "
                        + "table_id INTEGER, path TEXT, path_is_relative INTEGER, "
                        + "file_format TEXT, record_count INTEGER, file_size_bytes INTEGER, "
                        + "mapping_id INTEGER, partition_id INTEGER, file_order INTEGER, "
                        + "begin_snapshot INTEGER, end_snapshot INTEGER)");
                s.execute("CREATE TABLE ducklake_delete_file ("
                        + "delete_file_id INTEGER PRIMARY KEY, "
                        + "table_id INTEGER, data_file_id INTEGER, "
                        + "path TEXT, path_is_relative INTEGER, "
                        + "format TEXT, delete_count INTEGER, "
                        + "begin_snapshot INTEGER, end_snapshot INTEGER)");

                // --- Metadata ---
                s.execute("INSERT INTO ducklake_metadata VALUES ('data_path', '/tmp/ducklake_data/', NULL)");

                // --- 3 snapshots ---
                s.execute("INSERT INTO ducklake_snapshot VALUES (1, '2026-01-01T00:00:00', 'create table')");
                s.execute("INSERT INTO ducklake_snapshot VALUES (2, '2026-01-02T00:00:00', 'insert batch 2')");
                s.execute("INSERT INTO ducklake_snapshot VALUES (3, '2026-01-03T00:00:00', 'insert batch 3')");

                // --- Schema (visible from snapshot 1) ---
                s.execute("INSERT INTO ducklake_schema VALUES (1, 'main', NULL, 0, 1, NULL)");

                // --- Table (visible from snapshot 1) ---
                s.execute("INSERT INTO ducklake_table VALUES "
                        + "(1, 'uuid-test-1', 'test_table', 1, 'test_table/', 1, 1, NULL)");

                // --- Columns (visible from snapshot 1) ---
                s.execute("INSERT INTO ducklake_column VALUES "
                        + "(1, 1, 'id', 'INTEGER', 0, NULL, NULL, 0, NULL, 1, NULL)");
                s.execute("INSERT INTO ducklake_column VALUES "
                        + "(2, 1, 'name', 'VARCHAR', 1, NULL, NULL, 1, NULL, 1, NULL)");

                // --- A column added at snapshot 3 ---
                s.execute("INSERT INTO ducklake_column VALUES "
                        + "(3, 1, 'score', 'DOUBLE', 2, NULL, NULL, 1, NULL, 3, NULL)");

                // --- Data files: one per snapshot ---
                s.execute("INSERT INTO ducklake_data_file VALUES "
                        + "(1, 1, 'file1.parquet', 1, 'PARQUET', 2, 1000, NULL, NULL, 0, 1, NULL)");
                s.execute("INSERT INTO ducklake_data_file VALUES "
                        + "(2, 1, 'file2.parquet', 1, 'PARQUET', 1, 500,  NULL, NULL, 1, 2, NULL)");
                s.execute("INSERT INTO ducklake_data_file VALUES "
                        + "(3, 1, 'file3.parquet', 1, 'PARQUET', 1, 500,  NULL, NULL, 2, 3, NULL)");
            }
        }

        backend = new DuckLakeMetadataBackend(catalogFile.getAbsolutePath(), null);
    }

    @After
    public void tearDown() throws Exception {
        if (backend != null) {
            backend.close();
        }
        if (catalogFile != null) {
            catalogFile.delete();
        }
    }

    // ---------------------------------------------------------------
    // Snapshot query tests
    // ---------------------------------------------------------------

    @Test
    public void testGetSnapshotAtVersion() throws Exception {
        SnapshotInfo s1 = backend.getSnapshotAtVersion(1);
        assertNotNull(s1);
        assertEquals(1, s1.snapshotId);
        assertEquals("2026-01-01T00:00:00", s1.snapshotTime);
        assertEquals("create table", s1.changes);

        SnapshotInfo s2 = backend.getSnapshotAtVersion(2);
        assertNotNull(s2);
        assertEquals(2, s2.snapshotId);
        assertEquals("2026-01-02T00:00:00", s2.snapshotTime);

        SnapshotInfo s3 = backend.getSnapshotAtVersion(3);
        assertNotNull(s3);
        assertEquals(3, s3.snapshotId);
    }

    @Test
    public void testGetSnapshotAtVersionNotFound() throws Exception {
        SnapshotInfo missing = backend.getSnapshotAtVersion(999);
        assertNull("Non-existent version should return null", missing);
    }

    @Test
    public void testGetSnapshotAtTime() throws Exception {
        // Exact match
        SnapshotInfo snap = backend.getSnapshotAtTime("2026-01-02T00:00:00");
        assertNotNull(snap);
        assertEquals(2, snap.snapshotId);

        // Between snapshot 2 and 3 -> should return snapshot 2
        SnapshotInfo between = backend.getSnapshotAtTime("2026-01-02T12:00:00");
        assertNotNull(between);
        assertEquals(2, between.snapshotId);

        // After all snapshots -> should return snapshot 3
        SnapshotInfo after = backend.getSnapshotAtTime("2026-12-31T23:59:59");
        assertNotNull(after);
        assertEquals(3, after.snapshotId);
    }

    @Test
    public void testGetSnapshotAtTimeBeforeAll() throws Exception {
        // Before any snapshot exists -> should return null
        SnapshotInfo tooEarly = backend.getSnapshotAtTime("2020-01-01T00:00:00");
        assertNull("Timestamp before all snapshots should return null", tooEarly);
    }

    @Test
    public void testListSnapshots() throws Exception {
        List<SnapshotInfo> snapshots = backend.listSnapshots();
        assertEquals(3, snapshots.size());
        assertEquals(1, snapshots.get(0).snapshotId);
        assertEquals(2, snapshots.get(1).snapshotId);
        assertEquals(3, snapshots.get(2).snapshotId);
        assertEquals("2026-01-01T00:00:00", snapshots.get(0).snapshotTime);
        assertEquals("2026-01-02T00:00:00", snapshots.get(1).snapshotTime);
        assertEquals("2026-01-03T00:00:00", snapshots.get(2).snapshotTime);
    }

    // ---------------------------------------------------------------
    // resolveSnapshotId tests
    // ---------------------------------------------------------------

    @Test
    public void testResolveSnapshotIdDefault() throws Exception {
        // Neither version nor time -> latest (snapshot 3)
        long id = backend.resolveSnapshotId(null, null);
        assertEquals(3, id);
    }

    @Test
    public void testResolveSnapshotIdByVersion() throws Exception {
        long id = backend.resolveSnapshotId("2", null);
        assertEquals(2, id);
    }

    @Test
    public void testResolveSnapshotIdByTime() throws Exception {
        long id = backend.resolveSnapshotId(null, "2026-01-01T12:00:00");
        assertEquals(1, id);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResolveSnapshotIdMutualExclusion() throws Exception {
        backend.resolveSnapshotId("1", "2026-01-01T00:00:00");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResolveSnapshotIdInvalidVersion() throws Exception {
        backend.resolveSnapshotId("999", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResolveSnapshotIdNonNumericVersion() throws Exception {
        backend.resolveSnapshotId("abc", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResolveSnapshotIdTimestampTooEarly() throws Exception {
        backend.resolveSnapshotId(null, "2020-01-01T00:00:00");
    }

    // ---------------------------------------------------------------
    // Data file visibility at different snapshots
    // ---------------------------------------------------------------

    @Test
    public void testGetDataFilesAtSnapshot1() throws Exception {
        List<DataFileInfo> files = backend.getDataFiles(1, 1);
        assertEquals("Snapshot 1 should have 1 data file", 1, files.size());
        assertEquals("file1.parquet", files.get(0).path);
        assertEquals(2, files.get(0).recordCount);
    }

    @Test
    public void testGetDataFilesAtSnapshot2() throws Exception {
        List<DataFileInfo> files = backend.getDataFiles(1, 2);
        assertEquals("Snapshot 2 should have 2 data files", 2, files.size());
        assertEquals("file1.parquet", files.get(0).path);
        assertEquals("file2.parquet", files.get(1).path);
    }

    @Test
    public void testGetDataFilesAtSnapshot3() throws Exception {
        List<DataFileInfo> files = backend.getDataFiles(1, 3);
        assertEquals("Snapshot 3 should have 3 data files", 3, files.size());
        assertEquals("file1.parquet", files.get(0).path);
        assertEquals("file2.parquet", files.get(1).path);
        assertEquals("file3.parquet", files.get(2).path);
    }

    @Test
    public void testGetDataFilesDefaultIsLatest() throws Exception {
        // No snapshot specified -> should use current (latest = 3)
        List<DataFileInfo> files = backend.getDataFiles(1);
        assertEquals("Default (latest) should have 3 data files", 3, files.size());
    }

    // ---------------------------------------------------------------
    // Column visibility at different snapshots (schema evolution)
    // ---------------------------------------------------------------

    @Test
    public void testGetColumnsAtSnapshot1() throws Exception {
        List<ColumnInfo> cols = backend.getColumns(1, 1);
        assertEquals("Snapshot 1 should have 2 columns", 2, cols.size());
        assertEquals("id", cols.get(0).name);
        assertEquals("name", cols.get(1).name);
    }

    @Test
    public void testGetColumnsAtSnapshot2() throws Exception {
        List<ColumnInfo> cols = backend.getColumns(1, 2);
        assertEquals("Snapshot 2 should still have 2 columns", 2, cols.size());
    }

    @Test
    public void testGetColumnsAtSnapshot3() throws Exception {
        // Column 'score' was added at snapshot 3
        List<ColumnInfo> cols = backend.getColumns(1, 3);
        assertEquals("Snapshot 3 should have 3 columns (score added)", 3, cols.size());
        assertEquals("id", cols.get(0).name);
        assertEquals("name", cols.get(1).name);
        assertEquals("score", cols.get(2).name);
    }

    // ---------------------------------------------------------------
    // Table visibility at snapshot
    // ---------------------------------------------------------------

    @Test
    public void testGetTableAtSnapshot() throws Exception {
        TableInfo t = backend.getTable("test_table", "main", 1);
        assertNotNull("Table should be visible at snapshot 1", t);
        assertEquals(1, t.tableId);
        assertEquals("test_table", t.name);
    }

    @Test
    public void testGetTableAtSnapshotBeforeCreation() throws Exception {
        // Table was created at snapshot 1, so snapshot 0 should not find it
        TableInfo t = backend.getTable("test_table", "main", 0);
        assertNull("Table should not be visible before it was created", t);
    }
}
