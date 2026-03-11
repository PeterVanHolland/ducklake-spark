package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.catalog.DuckLakeViewManager;
import io.ducklake.spark.catalog.DuckLakeViewManager.ViewInfo;
import io.ducklake.spark.catalog.DuckLakeTagManager;

import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for view management and table/column tags.
 */
public class DuckLakeViewsTagsTest {

    private String tempDir;
    private String catalogPath;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-views-tags-test-").toString();
        catalogPath = tempDir + "/test.ducklake";

        // Bootstrap catalog with DuckDB
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
            conn.createStatement().execute("INSTALL ducklake; LOAD ducklake;");
            conn.createStatement().execute(
                    "ATTACH 'ducklake:" + catalogPath +
                    "?data_path=" + tempDir + "/data/' AS dl;");
            conn.createStatement().execute("CREATE TABLE dl.main.users (id INTEGER, name VARCHAR, email VARCHAR)");
            conn.createStatement().execute("INSERT INTO dl.main.users VALUES (1, 'Alice', 'alice@test.com')");
            conn.createStatement().execute("INSERT INTO dl.main.users VALUES (2, 'Bob', 'bob@test.com')");
            conn.createStatement().execute("DETACH dl");
        }
    }

    @After
    public void tearDown() {
        deleteDir(new File(tempDir));
    }

    // ---------------------------------------------------------------
    // View tests
    // ---------------------------------------------------------------

    @Test
    public void testCreateAndGetView() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeViewManager viewMgr = new DuckLakeViewManager(backend);
            SchemaInfo schema = backend.getSchemaByName("main");

            long viewId = viewMgr.createView(schema.schemaId, "active_users",
                    "SELECT * FROM users WHERE id > 0", false);
            assertTrue(viewId > 0);

            ViewInfo view = viewMgr.getView(schema.schemaId, "active_users");
            assertNotNull(view);
            assertEquals("active_users", view.name);
            assertEquals("SELECT * FROM users WHERE id > 0", view.sql);
        }
    }

    @Test
    public void testListViews() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeViewManager viewMgr = new DuckLakeViewManager(backend);
            SchemaInfo schema = backend.getSchemaByName("main");

            viewMgr.createView(schema.schemaId, "view_a", "SELECT 1", false);
            viewMgr.createView(schema.schemaId, "view_b", "SELECT 2", false);

            List<ViewInfo> views = viewMgr.listViews(schema.schemaId);
            assertEquals(2, views.size());
        }
    }

    @Test
    public void testDropView() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeViewManager viewMgr = new DuckLakeViewManager(backend);
            SchemaInfo schema = backend.getSchemaByName("main");

            viewMgr.createView(schema.schemaId, "temp_view", "SELECT 1", false);
            assertNotNull(viewMgr.getView(schema.schemaId, "temp_view"));

            viewMgr.dropView(schema.schemaId, "temp_view");
            assertNull(viewMgr.getView(schema.schemaId, "temp_view"));
        }
    }

    @Test
    public void testCreateOrReplaceView() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeViewManager viewMgr = new DuckLakeViewManager(backend);
            SchemaInfo schema = backend.getSchemaByName("main");

            viewMgr.createView(schema.schemaId, "my_view", "SELECT 1 AS old", false);
            viewMgr.createView(schema.schemaId, "my_view", "SELECT 2 AS new", true);

            ViewInfo view = viewMgr.getView(schema.schemaId, "my_view");
            assertEquals("SELECT 2 AS new", view.sql);
        }
    }

    @Test(expected = java.sql.SQLException.class)
    public void testCreateDuplicateViewFails() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeViewManager viewMgr = new DuckLakeViewManager(backend);
            SchemaInfo schema = backend.getSchemaByName("main");

            viewMgr.createView(schema.schemaId, "dup_view", "SELECT 1", false);
            viewMgr.createView(schema.schemaId, "dup_view", "SELECT 2", false); // should throw
        }
    }

    // ---------------------------------------------------------------
    // Table tag tests
    // ---------------------------------------------------------------

    @Test
    public void testSetAndGetTableTag() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");

            tagMgr.setTableTag(table.tableId, "owner", "engineering");
            tagMgr.setTableTag(table.tableId, "pii", "true");

            Map<String, String> tags = tagMgr.getTableTags(table.tableId);
            assertEquals("engineering", tags.get("owner"));
            assertEquals("true", tags.get("pii"));
        }
    }

    @Test
    public void testOverwriteTableTag() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");

            tagMgr.setTableTag(table.tableId, "status", "draft");
            tagMgr.setTableTag(table.tableId, "status", "published");

            Map<String, String> tags = tagMgr.getTableTags(table.tableId);
            assertEquals("published", tags.get("status"));
        }
    }

    @Test
    public void testDeleteTableTag() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");

            tagMgr.setTableTag(table.tableId, "temp", "yes");
            tagMgr.deleteTableTag(table.tableId, "temp");

            Map<String, String> tags = tagMgr.getTableTags(table.tableId);
            assertNull(tags.get("temp"));
        }
    }

    // ---------------------------------------------------------------
    // Column tag tests
    // ---------------------------------------------------------------

    @Test
    public void testSetAndGetColumnTag() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            // Find 'email' column
            long emailColId = -1;
            for (ColumnInfo col : columns) {
                if ("email".equals(col.name)) {
                    emailColId = col.columnId;
                    break;
                }
            }
            assertTrue(emailColId >= 0);

            tagMgr.setColumnTag(table.tableId, emailColId, "pii", "true");
            tagMgr.setColumnTag(table.tableId, emailColId, "sensitivity", "high");

            Map<String, String> tags = tagMgr.getColumnTags(table.tableId, emailColId);
            assertEquals("true", tags.get("pii"));
            assertEquals("high", tags.get("sensitivity"));
        }
    }

    // ---------------------------------------------------------------
    // Comment tests (sugar on top of tags)
    // ---------------------------------------------------------------

    @Test
    public void testTableComment() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");

            tagMgr.setTableComment(table.tableId, "Main user table for the application");
            assertEquals("Main user table for the application", tagMgr.getTableComment(table.tableId));
        }
    }

    @Test
    public void testColumnComment() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, tempDir + "/data/")) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            long nameColId = -1;
            for (ColumnInfo col : columns) {
                if ("name".equals(col.name)) {
                    nameColId = col.columnId;
                    break;
                }
            }

            tagMgr.setColumnComment(table.tableId, nameColId, "User's display name");
            assertEquals("User's display name", tagMgr.getColumnComment(table.tableId, nameColId));
        }
    }

    // ---------------------------------------------------------------

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) deleteDir(f);
            }
        }
        dir.delete();
    }
}
