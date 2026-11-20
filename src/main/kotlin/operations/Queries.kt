package org.iotsplab.akiba.dbDaemon.operations

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.http.HttpStatusCode
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger
import org.iotsplab.akiba.dbDaemon.RouteContext
import org.postgresql.util.PGobject
import java.sql.ResultSet
import java.sql.SQLException

object Queries {

    // ============================================================
    // -------------------------- ROUTES --------------------------
    // ============================================================

    object SelectIdInSQL : AbstractPostRoute() {
        override val path: String = "/get/id/sql"

        init {
            // Considering that receiving SQL commands from user is critically dangerous,
            // this route is disabled on default
            disabled = true
        }

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val data = ctx.receive<Map<String, String>>()["sql"]
                ?: return HttpStatusCode.BadRequest.description("SQL command is not provided")
            val ids: MutableSet<Long> = mutableSetOf()
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            fa.session.useDb { conn ->
                globalLogger.debug("SELECT u.id from using_binaries u $data")
                conn.prepareStatement("SELECT u.id from using_binaries u $data").use { stmt ->
                    val rs = stmt.executeQuery()
                    while (rs.next())
                        ids.add(rs.getLong("id"))
                }
            }

            return ids.toList()
        }
    }

    /**
     * Get a page of binary IDs ordered by id, with offset and limit.
     *
     * Accepts `{ "offset": 0, "limit": 20 }`. Both fields are integers and are
     * passed through a parameterized query — no SQL injection risk.
     */
    object SelectIdPage : AbstractPostRoute() {
        override val path: String = "/get/id/page"

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val data = ctx.receive<Map<String, Int>>()
            val offset = data["offset"] ?: 0
            val limit = data["limit"] ?: 20
            val ids: MutableList<Long> = mutableListOf()
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT u.id FROM using_binaries u ORDER BY u.id LIMIT ? OFFSET ?"
                ).use { stmt ->
                    stmt.setInt(1, limit)
                    stmt.setInt(2, offset)
                    val rs = stmt.executeQuery()
                    while (rs.next())
                        ids.add(rs.getLong("id"))
                }
            }

            return ids
        }
    }

    /**
     * Get total count of binary entries.
     *
     * Accepts an empty body. Returns a single integer.
     */
    object SelectIdCount : AbstractPostRoute() {
        override val path: String = "/get/id/count"

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }
            var count: Long = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT COUNT(*) FROM using_binaries"
                ).use { stmt ->
                    val rs = stmt.executeQuery()
                    if (rs.next())
                        count = rs.getLong(1)
                }
            }
            return count
        }
    }

    object GetBinaryMetadata : AbstractPostRoute() {
        override val path: String = "/get/metadata"

        const val QUERY_BINARY_SQL: String = """
            SELECT u.original_path original_path, u.arch arch, u.format format, 
               u.compiler_spec compiler_spec, u.checksum checksum, u.size size
            FROM binaries u
            WHERE u.id = ?
        """

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val id = ctx.receive<Long>()
            val metadata: MutableMap<String, Any?> = mutableMapOf()
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            fa.session.useDb { conn ->
                conn.prepareStatement(QUERY_BINARY_SQL).use { stmt ->
                    stmt.setLong(1, id)
                    val rs = stmt.executeQuery()
                    if (rs.next()) {
                        metadata["id"] = id
                        metadata["originalPath"] = rs.getString("original_path")
                        metadata["arch"] = rs.getString("arch")
                        metadata["format"] = rs.getString("format")
                        metadata["compilerSpec"] = rs.getString("compiler_spec")
                        metadata["checksum"] = rs.getString("checksum")
                        metadata["size"] = rs.getObject("size")
                    }
                }
            }

            return metadata
        }
    }

    /**
     * Search binaries by name, id, architecture, or format.
     *
     * Accepts `{ "query": "search_term" }`. Returns matching binary IDs
     * with basic metadata (name, arch, format, checksum).
     *
     * All user input goes through parameterized LIKE patterns — no SQL injection risk.
     */
    /**
     * Rows of the binaries table matching a WHERE condition.
     *
     * The condition comes from a plan script (`binaries("size > 1024")`), so it is read as
     * SQL on purpose: the table is one the operator already knows and this deployment is
     * single-user. It is placed inside a SELECT over the view, and a condition carrying a
     * statement separator is refused, which is the only way one could become a second
     * statement.
     */
    object SelectBinariesSQL : AbstractPostRoute() {
        override val path: String = "/get/binaries/sql"

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val where = ctx.receive<Map<String, String>>()["where"].orEmpty().trim()
            if (where.contains(';'))
                return HttpStatusCode.BadRequest.description("a WHERE condition cannot contain ';'")

            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            val clause = if (where.isBlank()) "" else " WHERE ($where)"
            val sql = "SELECT * FROM using_binaries$clause ORDER BY id"
            globalLogger.debug(sql)

            val rows = mutableListOf<Map<String, Any?>>()
            fa.session.useDb { conn ->
                conn.prepareStatement(sql).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        val meta = rs.metaData
                        while (rs.next()) {
                            val row = LinkedHashMap<String, Any?>()
                            for (i in 1..meta.columnCount) row[meta.getColumnName(i)] = rs.getObject(i)
                            rows.add(row)
                        }
                    }
                }
            }
            return rows
        }
    }

    object SearchBinaries : AbstractPostRoute() {
        override val path: String = "/get/search"

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val data = ctx.receive<Map<String, String>>()
            val query = (data["query"] ?: "").trim()
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            val likePattern = "%$query%"
            val numericId = query.toLongOrNull()
            // A 32-char hex string is treated as an MD5 checksum and
            // matched EXACTLY against u.checksum (used by modules to
            // locate an already-imported binary after a duplicate
            // import attempt).
            val isMd5 = query.length == 32 &&
                query.all { it.isDigit() || it.lowercaseChar() in 'a'..'f' }

            fa.session.useDb { conn ->
                val sql = buildString {
                    append("SELECT u.id, u.original_path, u.arch, u.format, u.compiler_spec, u.checksum ")
                    append("FROM using_binaries u ")
                    val conditions = when {
                        numericId != null ->
                            "WHERE u.id = ? OR u.original_path ILIKE ? OR u.arch ILIKE ? OR u.format ILIKE ?"
                        isMd5 ->
                            "WHERE u.checksum = ? OR u.original_path ILIKE ? OR u.arch ILIKE ? OR u.format ILIKE ?"
                        query.isNotEmpty() ->
                            "WHERE u.original_path ILIKE ? OR u.arch ILIKE ? OR u.format ILIKE ?"
                        else -> ""
                    }
                    append(conditions)
                    append(" ORDER BY u.id LIMIT 50")
                }

                conn.prepareStatement(sql).use { stmt ->
                    var idx = 1
                    if (numericId != null) {
                        stmt.setLong(idx++, numericId)
                    } else if (isMd5) {
                        stmt.setString(idx++, query.lowercase())
                    }
                    if (query.isNotEmpty()) {
                        stmt.setObject(idx++, likePattern, java.sql.Types.VARCHAR)
                        stmt.setObject(idx++, likePattern, java.sql.Types.VARCHAR)
                        stmt.setObject(idx++, likePattern, java.sql.Types.VARCHAR)
                    }
                    val rs = stmt.executeQuery()
                    while (rs.next()) {
                        results.add(mapOf(
                            "id" to rs.getLong("id"),
                            "name" to (rs.getString("original_path")?.substringAfterLast("/")
                                ?: rs.getString("original_path") ?: "unknown"),
                            "originalPath" to rs.getString("original_path"),
                            "arch" to rs.getString("arch"),
                            "format" to rs.getString("format"),
                            "compilerSpec" to rs.getString("compiler_spec"),
                            "checksum" to rs.getString("checksum")
                        ))
                    }
                }
            }

            return results
        }
    }

    /**
     * Delete binaries by id list.
     *
     * Accepts `{ "ids": [1, 2, ...] }`.  Ids are bound through a
     * parameterized `ANY(?)` array — no SQL injection risk.
     *
     * Deleting from `binaries` cascades to `results` (it references
     * `binaries.id` with ON DELETE CASCADE)
     * and resets child rows' `source_id` to NULL (ON DELETE SET NULL),
     * so one statement cleans every FK-linked row in this database.
     * Module-owned result tables and the agent database are NOT touched
     * (they are outside the FK graph).
     *
     * Returns the ids that were actually deleted (unknown ids are
     * silently skipped).  At most 500 ids per call.
     */
    /**
     * Correct the recorded metadata of a binary.
     *
     * The importer guesses: an unrecognised format comes in as `arch = "n/a"`, and Ghidra
     * then cannot pick a language. Letting the row be corrected is the difference between
     * a two-minute fix and a psql session inside the container.
     *
     * Only these three columns can be written, they are whitelisted by name, and each
     * value is bound as a parameter — nothing here builds SQL from the request body.
     */
    object UpdateBinaryMetadata : AbstractPostRoute() {
        override val path: String = "/set/binary-metadata"

        /** Longest value each column accepts; the schema has no limit, the UI does. */
        private const val MAX_VALUE_LENGTH = 200

        override suspend fun handle(ctx: RouteContext): Any? {
            val body = runCatching { ctx.receive<Map<String, Any?>>() }.getOrNull()
                ?: return HttpStatusCode.BadRequest.description("Expected a JSON object")
            val id = (body["id"] as? Number)?.toLong()
                ?: return HttpStatusCode.BadRequest.description("'id' is required")

            val columns = listOf("arch", "format", "compilerSpec")
                .mapNotNull { key ->
                    val value = body[key] as? String ?: return@mapNotNull null
                    val column = when (key) {
                        "arch" -> "arch"
                        "format" -> "format"
                        else -> "compiler_spec"
                    }
                    if (value.length > MAX_VALUE_LENGTH)
                        return HttpStatusCode.BadRequest.description("'$key' is too long")
                    column to value.trim()
                }
            if (columns.isEmpty())
                return HttpStatusCode.BadRequest.description(
                    "Nothing to update: send arch, format or compilerSpec")

            val assignments = columns.joinToString(", ") { "${it.first} = ?" }
            val sql = "UPDATE binaries SET $assignments WHERE id = ? RETURNING id"

            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            var updated = false
            fa.session.useDb { conn ->
                conn.prepareStatement(sql).use { stmt ->
                    columns.forEachIndexed { index, pair -> stmt.setString(index + 1, pair.second) }
                    stmt.setLong(columns.size + 1, id)
                    stmt.executeQuery().use { rs -> updated = rs.next() }
                }
            }
            if (!updated) return HttpStatusCode.NotFound.description("No binary with id $id")
            return mapOf("id" to id, "updated" to columns.map { it.first })
        }
    }

    /**
     * Every row that belongs to one binary, table by table.
     *
     * A binary id is not only a row in `binaries`: sessions, executions and whatever a
     * module writes all point at it. Showing only the `binaries` row hid the rest of the
     * record, which is what a person needs when deciding whether an import was right.
     */
    object GetBinaryRecords : AbstractPostRoute() {
        override val path: String = "/get/binary-records"

        private const val MAX_ROWS_PER_TABLE = 50

        override suspend fun handle(ctx: RouteContext): Any? {
            val body = runCatching { ctx.receive<Map<String, Any?>>() }.getOrNull()
                ?: return HttpStatusCode.BadRequest.description("Expected a JSON object")
            val id = (body["id"] as? Number)?.toLong()
                ?: return HttpStatusCode.BadRequest.description("'id' is required")

            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            val tables = mutableListOf<Map<String, Any?>>()
            fa.session.useDb { conn ->
                // Anything that points at a binary, plus the binaries row itself.
                val candidates = mutableListOf("binaries")
                conn.prepareStatement(
                    "SELECT table_name FROM information_schema.columns " +
                        "WHERE table_schema = 'public' AND column_name = 'binary_id' ORDER BY table_name"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        while (rs.next()) {
                            val name = rs.getString(1)
                            if (name != "binaries") candidates.add(name)
                        }
                    }
                }

                for (table in candidates) {
                    if (!TABLE_NAME.matches(table)) continue
                    val columns = mutableListOf<Map<String, String>>()
                    conn.prepareStatement(
                        "SELECT column_name, data_type FROM information_schema.columns " +
                            "WHERE table_schema = 'public' AND table_name = ? ORDER BY ordinal_position"
                    ).use { stmt ->
                        stmt.setString(1, table)
                        stmt.executeQuery().use { rs ->
                            while (rs.next()) columns.add(
                                mapOf("name" to rs.getString(1), "type" to rs.getString(2))
                            )
                        }
                    }
                    if (columns.isEmpty()) continue

                    val where = if (table == "binaries") "id = ?" else "binary_id = ?"
                    val rows = mutableListOf<Map<String, Any?>>()
                    conn.prepareStatement("SELECT * FROM $table WHERE $where LIMIT $MAX_ROWS_PER_TABLE")
                        .use { stmt ->
                            stmt.setLong(1, id)
                            stmt.executeQuery().use { rs ->
                                val meta = rs.metaData
                                while (rs.next()) {
                                    val values = linkedMapOf<String, Any?>()
                                    val lengths = linkedMapOf<String, Int>()
                                    for (i in 1..meta.columnCount) {
                                        val name = meta.getColumnName(i)
                                        val raw = rs.getObject(i)
                                        values[name] = displayValue(raw)
                                        // The browser gets a prefix, so the real length
                                        // travels beside it — a value cut at 4 KB would
                                        // otherwise never look like the megabyte it is.
                                        val trueLength = when (raw) {
                                            null -> 0
                                            is String -> raw.length
                                            is ByteArray -> raw.size
                                            else -> 0
                                        }
                                        if (trueLength > MAX_TEXT) lengths[name] = trueLength
                                    }
                                    rows.add(mapOf("values" to values, "lengths" to lengths))
                                }
                            }
                        }
                    if (rows.isEmpty()) continue
                    tables.add(mapOf("name" to table, "columns" to columns, "rows" to rows))
                }
            }
            return mapOf("id" to id, "tables" to tables)
        }

        /** Long text is shortened here: the browser should not receive megabytes. */
        private fun displayValue(value: Any?): Any? = when (value) {
            null -> null
            is ByteArray -> "<${value.size} bytes>"
            is String -> if (value.length > MAX_TEXT) value.take(MAX_TEXT) else value
            else -> value
        }

        private const val MAX_TEXT = 4096
        private val TABLE_NAME = Regex("^[a-z_][a-z0-9_]*$")
    }

    /**
     * Change one column of one row.
     *
     * The table and column are read from the catalogue, never trusted from the body:
     * anything not in `information_schema` is refused before a statement is built. Values
     * are always bound.
     */
    object UpdateRecord : AbstractPostRoute() {
        override val path: String = "/set/record"

        /** Generated or derived columns: a correction must not rewrite history or identity. */
        private val READ_ONLY_BINARIES = setOf("id", "size", "checksum", "original_path")

        override suspend fun handle(ctx: RouteContext): Any? {
            val body = runCatching { ctx.receive<Map<String, Any?>>() }.getOrNull()
                ?: return HttpStatusCode.BadRequest.description("Expected a JSON object")
            val table = (body["table"] as? String)?.trim()
                ?: return HttpStatusCode.BadRequest.description("'table' is required")
            val column = (body["column"] as? String)?.trim()
                ?: return HttpStatusCode.BadRequest.description("'column' is required")
            val id = (body["id"] as? Number)?.toLong()
                ?: return HttpStatusCode.BadRequest.description("'id' is required")
            val value = body["value"] as? String
                ?: return HttpStatusCode.BadRequest.description("'value' must be a string")

            if (!TABLE_NAME.matches(table) || !TABLE_NAME.matches(column))
                return HttpStatusCode.BadRequest.description("Unsupported table or column name")
            if (table == "binaries" && column in READ_ONLY_BINARIES)
                return HttpStatusCode.BadRequest.description("'$column' cannot be edited")
            if (value.length > MAX_VALUE_LENGTH)
                return HttpStatusCode.BadRequest.description("Value is too long")

            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            var updated = false
            fa.session.useDb { conn ->
                val exists = conn.prepareStatement(
                    "SELECT data_type FROM information_schema.columns " +
                        "WHERE table_schema = 'public' AND table_name = ? AND column_name = ?"
                ).use { stmt ->
                    stmt.setString(1, table)
                    stmt.setString(2, column)
                    stmt.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
                } ?: return@useDb

                // Numeric columns keep their type; everything else takes the text.
                val sql = if (exists.contains("int") || exists.contains("numeric") || exists.contains("double"))
                    "UPDATE $table SET $column = CAST(? AS ${exists}) WHERE id = ? RETURNING id"
                else
                    "UPDATE $table SET $column = ? WHERE id = ? RETURNING id"

                conn.prepareStatement(sql).use { stmt ->
                    stmt.setString(1, value)
                    stmt.setLong(2, id)
                    stmt.executeQuery().use { rs -> updated = rs.next() }
                }
            }
            if (!updated) return HttpStatusCode.NotFound.description("No row $id in $table")
            return mapOf("table" to table, "column" to column, "id" to id, "updated" to true)
        }

        private const val MAX_VALUE_LENGTH = 100_000
        private val TABLE_NAME = Regex("^[a-z_][a-z0-9_]*$")
    }

    object DeleteBinaries : AbstractPostRoute() {
        override val path: String = "/delete/binaries"

        const val MAX_IDS_PER_CALL: Int = 500

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val data = ctx.receive<Map<String, List<Long>>>()
            val ids = data["ids"]?.filter { it > 0 }?.distinct().orEmpty()
            if (ids.isEmpty())
                return HttpStatusCode.BadRequest.description("No binary ids provided")
            if (ids.size > MAX_IDS_PER_CALL)
                return HttpStatusCode.BadRequest.description(
                    "Too many ids (max $MAX_IDS_PER_CALL per call)")

            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            val deleted: MutableList<Long> = mutableListOf()
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "DELETE FROM binaries WHERE id = ANY(?) RETURNING id"
                ).use { stmt ->
                    stmt.setArray(1, conn.createArrayOf("int4", ids.toTypedArray()))
                    val rs = stmt.executeQuery()
                    while (rs.next())
                        deleted.add(rs.getLong("id"))
                }
            }
            globalLogger.info(
                "DeleteBinaries: requested {}, deleted {} ({})",
                ids.size, deleted.size, deleted)
            return deleted
        }
    }

    /**
     * Get module data from module table
     *
     * Data format:
     *
     * ```json
     * {
     *   "results": [
     *     {"name": "flag", "type": "int4", "value": 123},
     *     {"name": "work_done", "type": "bool", "value": true}
     *   ]
     * }
     * ```
     */
    object GetModuleData : AbstractPostRoute() {
        override val path: String = "/get/module_data"

        val reservedColumns = listOf("id", "start_timestamp", "finish_timestamp", "execute_time")

        const val QUERY_SQL: String = """
            SELECT %s FROM %s WHERE id = %d;
        """
        const val QUERY_TYPE_SQL: String = """
            SELECT pg_typeof(%s) FROM (SELECT %s FROM %s LIMIT 1);
        """
        const val QUERY_ALL_COLUMN_TYPES_SQL: String = """
            SELECT column_name, data_type FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = '%s';
        """

        data class QueryTarget (
            val tableName: String,
            // Considering that query data from many ids may cause the response too long
            // we only offer this query method which can only query on one id at a time
            val id: Long,
            val columns: List<String>?
        )

        data class ModuleData(val name: String, val type: String, val value: Any?)

        data class QueryResults (var results: MutableList<ModuleData> = mutableListOf())

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val target = ctx.receive<QueryTarget>()
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            // TODO: The cost checking tables and columns every time is too expensive, how to optimize?
            if (!fa.session.existsTable(target.tableName) &&
                !fa.session.existsView(target.tableName))
                return HttpStatusCode.NotFound.description("Table or view ${target.tableName} not found")
            if (target.columns?.isNotEmpty() == true) {
                target.columns.forEach {
                    if (it in reservedColumns)
                        return HttpStatusCode.BadRequest.description("Column $it is reserved")
                    if (!fa.session.existsColumn(target.tableName, it))
                        return HttpStatusCode.NotFound.description("Column $it does not exist")
                }
            }

            val result = QueryResults()

            // column names, data types
            val columns: MutableMap<String, String> = mutableMapOf()
            target.columns ?.let {
                it.forEach { columnName ->
                    fa.session.useDb { conn ->
                        conn.prepareStatement(
                            QUERY_TYPE_SQL.format(columnName, columnName, target.tableName)).use { stmt ->
                            val rs = stmt.executeQuery()
                            while (rs.next())
                                columns[columnName] = rs.getString(1)
                        }
                    }
                }
            } ?: run {
                fa.session.useDb { conn ->
                    conn.prepareStatement(
                        QUERY_ALL_COLUMN_TYPES_SQL.format(target.tableName)).use { stmt ->
                        val rs = stmt.executeQuery()
                        while (rs.next()) {
                            val columnName = rs.getString("column_name")
                            if (columnName !in reservedColumns)
                                columns[columnName] = rs.getString("data_type")
                        }
                    }
                }
            }

            // Considering that we have checked the validity of the table and columns,
            // there is no safety issues to execute the joined query
            columns.forEach { column, type ->
                fa.session.useDb { conn ->
                    conn.prepareStatement(QUERY_SQL.format(
                        column,     // Columns
                        target.tableName,   // Table
                        target.id           // ID
                    )).use { stmt ->
                        val rs = stmt.executeQuery()
                        if (rs.next()) {
                            val data = mutableMapOf<String, Any?>()
                            for (i in 1..rs.metaData.columnCount) {
                                val obj = rs.getObject(i)
                                data[rs.metaData.getColumnName(i)] = if (obj is PGobject) obj.value else rs.getObject(i)
                            }
                            result.results.add(ModuleData(column, type, data[column]))
                        }
                    }
                }
            }

            return result
        }
    }
}