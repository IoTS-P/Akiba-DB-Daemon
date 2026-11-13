package org.iotsplab.akiba.dbDaemon.operations

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.http.HttpStatusCode
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger
import org.iotsplab.akiba.dbDaemon.RouteContext
import org.postgresql.util.PGobject
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
               u.compiler_spec compiler_spec, u.checksum checksum,
               u.processed_path processed_path, u.processed_checksum processed_checksum
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
                        metadata["processedPath"] = rs.getString("processed_path")
                        metadata["processedChecksum"] = rs.getString("processed_checksum")
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