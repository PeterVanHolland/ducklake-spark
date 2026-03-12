package io.ducklake.spark.catalog;

import io.ducklake.spark.catalog.DuckLakeMetadataBackend.*;
import io.ducklake.spark.util.DuckLakeTypeMapping;

import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import io.ducklake.spark.reader.DuckLakeScanBuilder;
import io.ducklake.spark.writer.DuckLakeDeleteExecutor;
import io.ducklake.spark.writer.DuckLakeWriteBuilder;

import java.sql.SQLException;
import java.util.*;

/**
 * Spark CatalogPlugin for DuckLake.
 *
 * Allows using DuckLake tables via Spark SQL:
 * <pre>
 *   spark.conf.set("spark.sql.catalog.ducklake", "io.ducklake.spark.catalog.DuckLakeCatalog")
 *   spark.conf.set("spark.sql.catalog.ducklake.catalog", "/path/to/catalog.ducklake")
 *
 *   spark.sql("CREATE TABLE ducklake.main.t (id INT, name STRING)")
 *   spark.sql("INSERT INTO ducklake.main.t VALUES (1, 'hello')")
 *   spark.sql("SELECT * FROM ducklake.main.t")
 * </pre>
 */
public class DuckLakeCatalog implements CatalogPlugin, TableCatalog, SupportsNamespaces {

    private String catalogName;
    private CaseInsensitiveStringMap options;
    private String catalogPath;
    private String dataPath;
    /** In-memory metadata cache — mirrors Iceberg's CachingCatalog.
     *  Key = "schema.table", value = fully-resolved metadata (files, stats, mappings).
     *  Invalidated on any write/DDL operation. */
    private final java.util.concurrent.ConcurrentHashMap<String, DuckLakeTableMetadataCache> metaCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.options = options;
        this.catalogPath = options.get("catalog");
        if (this.catalogPath == null || this.catalogPath.isEmpty()) {
            throw new IllegalArgumentException(
                    "Option 'catalog' is required for DuckLakeCatalog " +
                    "(path to .ducklake file or PostgreSQL DSN)");
        }
        this.dataPath = options.getOrDefault("data_path", null);
    }

    @Override
    public String name() {
        return catalogName;
    }

    // ---------------------------------------------------------------
    // TableCatalog
    // ---------------------------------------------------------------

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        String schemaName = resolveSchemaName(namespace);
        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) {
                throw new NoSuchNamespaceException(namespace);
            }
            List<TableInfo> tables = backend.listTables(schema.schemaId);
            Identifier[] result = new Identifier[tables.size()];
            for (int i = 0; i < tables.size(); i++) {
                result[i] = Identifier.of(namespace, tables.get(i).name);
            }
            return result;
        } catch (NoSuchNamespaceException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list tables in " + schemaName, e);
        }
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        String schemaName = resolveSchemaName(ident.namespace());
        String tableName = ident.name();
        String cacheKey = schemaName + "." + tableName;

        // Fast path: return from in-memory cache (zero SQLite)
        DuckLakeTableMetadataCache cached = metaCache.get(cacheKey);
        if (cached != null) {
            StructType sparkSchema = DuckLakeTypeMapping.buildSchema(cached.columns);
            TableInfo tableInfo = new TableInfo(cached.tableId, null,
                    cached.tableName, cached.tablePath, cached.pathIsRelative);
            return new DuckLakeCatalogTable(ident, sparkSchema, tableInfo,
                    buildTableOptions(schemaName, tableName, cached.tableId),
                    cached);
        }

        // Cold path: load from SQLite, populate cache
        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) {
                throw new NoSuchTableException(ident);
            }
            TableInfo tableInfo = backend.getTable(schema.schemaId, tableName);
            if (tableInfo == null) {
                throw new NoSuchTableException(ident);
            }
            long snapshotId = backend.getCurrentSnapshotId();

            // Full cache: table + columns + files + stats + delete files + name mappings
            DuckLakeTableMetadataCache fullCache =
                    DuckLakeTableMetadataCache.load(backend, tableInfo, snapshotId);
            metaCache.put(cacheKey, fullCache);

            StructType sparkSchema = DuckLakeTypeMapping.buildSchema(fullCache.columns);
            return new DuckLakeCatalogTable(ident, sparkSchema, tableInfo,
                    buildTableOptions(schemaName, tableName, tableInfo.tableId),
                    fullCache);
        } catch (NoSuchTableException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load table " + ident, e);
        }
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
        metaCache.clear(); // invalidate on schema change
        String schemaName = resolveSchemaName(ident.namespace());
        String tableName = ident.name();

        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schemaInfo = backend.getSchemaByName(schemaName);
            if (schemaInfo == null) {
                throw new NoSuchNamespaceException(ident.namespace());
            }

            // Check if table already exists
            TableInfo existing = backend.getTable(schemaInfo.schemaId, tableName);
            if (existing != null) {
                throw new TableAlreadyExistsException(ident);
            }

            // Convert Spark schema to DuckLake columns
            StructField[] fields = schema.fields();
            String[] colNames = new String[fields.length];
            String[] colTypes = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                colNames[i] = fields[i].name();
                colTypes[i] = DuckLakeTypeMapping.toDuckDBType(fields[i].dataType());
            }

            TableInfo tableInfo = backend.createTableEntry(schemaInfo.schemaId, tableName, colNames, colTypes);

            // Also create the data directory for the table
            String dp = backend.getDataPath();
            if (dp != null && !dp.isEmpty()) {
                new java.io.File(dp + tableInfo.path).mkdirs();
            }

            return new DuckLakeCatalogTable(ident, schema, tableInfo, buildTableOptions(schemaName, tableName));
        } catch (TableAlreadyExistsException | NoSuchNamespaceException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create table " + ident, e);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        metaCache.clear(); // invalidate on schema change
        String schemaName = resolveSchemaName(ident.namespace());
        String tableName = ident.name();

        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) {
                throw new NoSuchTableException(ident);
            }
            TableInfo tableInfo = backend.getTable(schema.schemaId, tableName);
            if (tableInfo == null) {
                throw new NoSuchTableException(ident);
            }

            for (TableChange change : changes) {
                if (change instanceof TableChange.AddColumn) {
                    TableChange.AddColumn add = (TableChange.AddColumn) change;
                    String[] fieldNames = add.fieldNames();
                    String colName = fieldNames[fieldNames.length - 1];
                    String colType = DuckLakeTypeMapping.toDuckDBType(add.dataType());
                    boolean nullable = add.isNullable();

                    // TODO: Extract default values from table properties or metadata when Spark supports it
                    String initialDefault = null;
                    String writeDefault = null;

                    backend.addColumnWithDefault(tableInfo.tableId, colName, colType, nullable, initialDefault, writeDefault);
                } else if (change instanceof TableChange.DeleteColumn) {
                    TableChange.DeleteColumn del = (TableChange.DeleteColumn) change;
                    String[] fieldNames = del.fieldNames();
                    String colName = fieldNames[fieldNames.length - 1];
                    backend.dropColumn(tableInfo.tableId, colName);
                } else if (change instanceof TableChange.RenameColumn) {
                    TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
                    String[] fieldNames = rename.fieldNames();
                    String oldName = fieldNames[fieldNames.length - 1];
                    backend.renameColumn(tableInfo.tableId, oldName, rename.newName());
                } else if (change instanceof TableChange.UpdateColumnType) {
                    TableChange.UpdateColumnType updateType = (TableChange.UpdateColumnType) change;
                    String[] fieldNames = updateType.fieldNames();
                    String colName = fieldNames[fieldNames.length - 1];
                    String newType = DuckLakeTypeMapping.toDuckDBType(updateType.newDataType());
                    backend.updateColumnType(tableInfo.tableId, colName, newType);
                } else if (change instanceof TableChange.SetProperty) {
                    TableChange.SetProperty setProp = (TableChange.SetProperty) change;
                    DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
                    tagMgr.setTableTag(tableInfo.tableId, setProp.property(), setProp.value());
                } else if (change instanceof TableChange.RemoveProperty) {
                    TableChange.RemoveProperty removeProp = (TableChange.RemoveProperty) change;
                    DuckLakeTagManager tagMgr = new DuckLakeTagManager(backend);
                    tagMgr.deleteTableTag(tableInfo.tableId, removeProp.property());
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported ALTER TABLE change: " + change.getClass().getSimpleName());
                }
            }

            // Reload the table with updated schema
            List<ColumnInfo> columns = backend.getColumns(tableInfo.tableId);
            StructType sparkSchema = DuckLakeTypeMapping.buildSchema(columns);
            return new DuckLakeCatalogTable(ident, sparkSchema, tableInfo, buildTableOptions(schemaName, tableName));
        } catch (NoSuchTableException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to alter table " + ident, e);
        }
    }

    @Override
    public boolean dropTable(Identifier ident) {
        metaCache.clear(); // invalidate on schema change
        String schemaName = resolveSchemaName(ident.namespace());
        String tableName = ident.name();

        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) return false;

            TableInfo tableInfo = backend.getTable(schema.schemaId, tableName);
            if (tableInfo == null) return false;

            return backend.dropTableEntry(tableInfo.tableId);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop table " + ident, e);
        }
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException("RENAME TABLE is not yet supported by DuckLakeCatalog");
    }

    @Override
    public boolean tableExists(Identifier ident) {
        String schemaName = resolveSchemaName(ident.namespace());
        String tableName = ident.name();

        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) return false;

            return backend.getTable(schema.schemaId, tableName) != null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check table existence: " + ident, e);
        }
    }

    // ---------------------------------------------------------------
    // SupportsNamespaces
    // ---------------------------------------------------------------

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        try (DuckLakeMetadataBackend backend = createBackend()) {
            List<SchemaInfo> schemas = backend.listSchemas();
            String[][] result = new String[schemas.size()][];
            for (int i = 0; i < schemas.size(); i++) {
                result[i] = new String[]{schemas.get(i).name};
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list namespaces", e);
        }
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        String schemaName = namespace[0];
        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) {
                throw new NoSuchNamespaceException(namespace);
            }
        } catch (NoSuchNamespaceException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list namespaces", e);
        }
        return new String[0][];
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        String schemaName = resolveSchemaName(namespace);
        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) {
                throw new NoSuchNamespaceException(namespace);
            }
            Map<String, String> metadata = new HashMap<>();
            metadata.put("schema_id", String.valueOf(schema.schemaId));
            metadata.put("path", schema.path != null ? schema.path : "");
            return metadata;
        } catch (NoSuchNamespaceException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load namespace metadata", e);
        }
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        if (namespace.length != 1) {
            throw new IllegalArgumentException("DuckLake supports single-level namespaces only");
        }
        String schemaName = namespace[0];
        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo existing = backend.getSchemaByName(schemaName);
            if (existing != null) {
                throw new NamespaceAlreadyExistsException(namespace);
            }
            backend.createSchema(schemaName);
        } catch (NamespaceAlreadyExistsException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create namespace " + schemaName, e);
        }
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("ALTER NAMESPACE is not yet supported by DuckLakeCatalog");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) throws NoSuchNamespaceException, NonEmptyNamespaceException {
        if (namespace.length != 1) {
            return false;
        }
        String schemaName = namespace[0];
        try (DuckLakeMetadataBackend backend = createBackend()) {
            SchemaInfo schema = backend.getSchemaByName(schemaName);
            if (schema == null) {
                throw new NoSuchNamespaceException(namespace);
            }

            if (!cascade) {
                List<TableInfo> tables = backend.listTables(schema.schemaId);
                if (!tables.isEmpty()) {
                    throw new RuntimeException("Namespace " + schemaName +
                            " is not empty. Use CASCADE to drop it with all tables.");
                }
            }

            backend.dropSchema(schema.schemaId);
            return true;
        } catch (NoSuchNamespaceException e) {
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop namespace " + schemaName, e);
        }
    }



    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private DuckLakeMetadataBackend createBackend() {
        return new DuckLakeMetadataBackend(catalogPath, dataPath);
    }

    private String resolveSchemaName(String[] namespace) {
        if (namespace == null || namespace.length == 0) {
            return "main";
        }
        return namespace[0];
    }

    private CaseInsensitiveStringMap buildTableOptions(String schemaName, String tableName) {
        return buildTableOptions(schemaName, tableName, -1);
    }

    private CaseInsensitiveStringMap buildTableOptions(String schemaName, String tableName, long tableId) {
        Map<String, String> opts = new HashMap<>();
        opts.put("catalog", catalogPath);
        if (dataPath != null) {
            opts.put("data_path", dataPath);
        }
        opts.put("table", tableName);
        opts.put("schema", schemaName);
        if (tableId >= 0) opts.put("_table_id", String.valueOf(tableId));
        for (Map.Entry<String, String> entry : options.asCaseSensitiveMap().entrySet()) {
            opts.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return new CaseInsensitiveStringMap(opts);
    }

    // ---------------------------------------------------------------
    // Inner Table class for catalog-managed tables
    // ---------------------------------------------------------------

    static class DuckLakeCatalogTable implements Table, SupportsRead, SupportsWrite, SupportsDelete {
        private final Identifier ident;
        private final StructType schema;
        private final TableInfo tableInfo;
        private final CaseInsensitiveStringMap options;
        private final DuckLakeTableMetadataCache metadataCache;

        DuckLakeCatalogTable(Identifier ident, StructType schema,
                             TableInfo tableInfo, CaseInsensitiveStringMap options) {
            this(ident, schema, tableInfo, options, null);
        }

        DuckLakeCatalogTable(Identifier ident, StructType schema,
                             TableInfo tableInfo, CaseInsensitiveStringMap options,
                             DuckLakeTableMetadataCache metadataCache) {
            this.ident = ident;
            this.schema = schema;
            this.tableInfo = tableInfo;
            this.options = options;
            this.metadataCache = metadataCache;
        }

        @Override
        public String name() {
            return ident.toString();
        }

        @Override
        public StructType schema() {
            return schema;
        }

        @Override
        public Set<TableCapability> capabilities() {
            return new HashSet<>(Arrays.asList(
                    TableCapability.BATCH_READ,
                    TableCapability.BATCH_WRITE,
                    TableCapability.TRUNCATE
            ));
        }

        @Override

        public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
            Map<String, String> merged = new HashMap<>(this.options.asCaseSensitiveMap());
            merged.putAll(scanOptions.asCaseSensitiveMap());
            return new DuckLakeScanBuilder(schema, new CaseInsensitiveStringMap(merged), metadataCache);
        }

        @Override
        public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
            Map<String, String> merged = new HashMap<>(this.options.asCaseSensitiveMap());
            merged.putAll(info.options().asCaseSensitiveMap());
            return new DuckLakeWriteBuilder(info.schema(), new CaseInsensitiveStringMap(merged));
        }

        @Override
        public boolean canDeleteWhere(Filter[] filters) {
            return true;
        }

        @Override
        public void deleteWhere(Filter[] filters) {
            new DuckLakeDeleteExecutor(
                    options.get("catalog"),
                    options.getOrDefault("data_path", null),
                    options.get("table"),
                    options.getOrDefault("schema", "main")
            ).deleteWhere(filters);
        }
    }
}
