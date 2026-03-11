package io.ducklake.spark;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend;
import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.catalog.DuckLakeViewManager;
import io.ducklake.spark.catalog.DuckLakeViewManager.ViewInfo;
import io.ducklake.spark.catalog.DuckLakeTagManager;

import org.junit.*;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests for view management and table/column tags.
 */
public class DuckLakeViewsTagsTest {

    private String tempDir;
    private String catalogPath;
    private String dataPath;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ducklake-views-tags-test-").toString();
        dataPath = tempDir + "/data/";
        new File(dataPath).mkdirs();
        new File(dataPath + "main/users/").mkdirs();
        catalogPath = tempDir + "/test.ducklake";

        createCatalog(catalogPath, dataPath);
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            DuckLakeViewManager viewMgr = new DuckLakeViewManager(backend);
            SchemaInfo schema = backend.getSchemaByName("main");

            viewMgr.createView(schema.schemaId, "dup_view", "SELECT 1", false);
            viewMgr.createView(schema.schemaId, "dup_view", "SELECT 2", false);
        }
    }

    // ---------------------------------------------------------------
    // Table tag tests
    // ---------------------------------------------------------------

    @Test
    public void testSetAndGetTableTag() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");
            List<ColumnInfo> columns = backend.getColumns(table.tableId);

            long emailColId = -1;
            for (ColumnInfo col : columns) {
                if ("email".equals(col.name)) {
                    emailColId = col.columnId;
                    break;
                }
            }
            assertTrue(emailColId >= 0);

            tagMgr.setColumnTag(table.tableId, emailColId, "pii", "true");

            Map<String, String> tags = tagMgr.getColumnTags(table.tableId, emailColId);
            assertEquals("true", tags.get("pii"));
        }
    }

    // ---------------------------------------------------------------
    // Comment tests
    // ---------------------------------------------------------------

    @Test
    public void testTableComment() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
            DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
            TableInfo table = backend.getTable("users", "main");

            tagMgr.setTableComment(table.tableId, "Main user table");
            assertEquals("Main user table", tagMgr.getTableComment(table.tableId));
        }
    }

    @Test
    public void testColumnComment() throws Exception {
        try (DuckLakeMetadataBackend backend = new DuckLakeMetadataBackend(catalogPath, dataPath)) {
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

            tagMgr.setColumnComment(table.tableId, nameColId, "User display name");
            assertEquals("User display name", tagMgr.getColumnComment(table.tableId, nameColId));
        }
    }

    // ---------------------------------------------------------------
    // Catalog bootstrap (raw SQLite, matching existing test patterns)
    // ---------------------------------------------------------------

    private void createCatalog(String catPath, String dp) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catPath)) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT)");
                st.execute("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TEXT, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)");
                st.execute("CREATE TABLE ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)");
                st.execute("CREATE TABLE ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                st.execute("CREATE TABLE ducklake_table(table_id BIGINT, table_uuid TEXT, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN)");
                st.execute("CREATE TABLE ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)");
                st.execute("CREATE TABLE ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT)");
                st.execute("CREATE TABLE ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR)");
                st.execute("CREATE TABLE ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)");
                st.execute("CREATE TABLE ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT)");
                st.execute("CREATE TABLE ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN)");
                st.execute("CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)");
                st.execute("CREATE TABLE ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)");

                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.4')");
                st.execute("INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', '" + dp + "')");

                st.execute("INSERT INTO ducklake_snapshot VALUES (0, datetime('now'), 0, 1, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:\"main\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_schema VALUES (0, 'schema-uuid-0', 0, NULL, 'main', 'main/', 1)");

                // Table: users (id, name, email)
                st.execute("INSERT INTO ducklake_snapshot VALUES (1, datetime('now'), 1, 5, 0)");
                st.execute("INSERT INTO ducklake_snapshot_changes VALUES (1, 'created_table:\"main\".\"users\"', NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_table VALUES (1, 'table-uuid-users', 1, NULL, 0, 'users', 'main/users/', 1)");
                st.execute("INSERT INTO ducklake_column VALUES (2, 1, NULL, 1, 0, 'id', 'INTEGER', NULL, NULL, 1, NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_column VALUES (3, 1, NULL, 1, 1, 'name', 'VARCHAR', NULL, NULL, 1, NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_column VALUES (4, 1, NULL, 1, 2, 'email', 'VARCHAR', NULL, NULL, 1, NULL, NULL, NULL)");
                st.execute("INSERT INTO ducklake_table_stats VALUES (1, 0, 0, 0)");
            }
            conn.commit();
        }
    }

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
