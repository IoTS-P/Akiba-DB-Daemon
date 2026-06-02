package org.iotsplab.akiba.dbDaemon.operations

import io.ktor.http.HttpStatusCode
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.RouteContext
import java.sql.Timestamp
import java.time.Instant

/**
 * Script-related database operations for the Akiba Agent Framework.
 *
 * Scripts and their execution records are stored **per PG instance** so that
 * each user's data is isolated and can be backed up/restored as a unit.
 * There is no `user_id` column — user ownership is tracked at the server
 * level (the default 5432 DB).
 *
 * All routes follow the same pattern as [AgentOps]:
 *   1. Parse request body
 *   2. Authenticate via [fastAccess]
 *   3. Execute SQL via [UserDatabaseSession.useDb]
 *   4. Return result
 */
object ScriptOps {

    // ============================================================
    //  Script CRUD
    // ============================================================

    /** Create a new script record (or update if same name+author exists). */
    object CreateScript : AbstractPostRoute() {
        override val path: String = "/script/create"

        data class Request(
            val name: String,
            val description: String = "",
            val author: String = "",
            val code: String,
            val language: String = "kotlin",
            val saveResult: Boolean = true,
            val maxOutputSize: Long = 10485760
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var scriptId: Int? = null
            fa.session.useDb { conn ->
                // Check if a script with the same name already exists
                conn.prepareStatement(
                    "SELECT id, author FROM scripts WHERE name = ?"
                ).use { ps ->
                    ps.setString(1, req.name)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        val existingId = rs.getInt("id")
                        val existingAuthor = rs.getString("author") ?: ""

                        if (existingAuthor == req.author) {
                            // Same author — update (overwrite)
                            conn.prepareStatement(
                                """
                                UPDATE scripts SET description = ?, code = ?, code_size = ?,
                                    language = ?, save_result = ?, max_output_size = ?,
                                    status = 'pending', output = NULL, output_size = 0, finished_at = NULL
                                WHERE id = ?
                                """.trimIndent()
                            ).use { ups ->
                                ups.setString(1, req.description)
                                ups.setString(2, req.code)
                                ups.setInt(3, req.code.toByteArray().size)
                                ups.setString(4, req.language)
                                ups.setBoolean(5, req.saveResult)
                                ups.setLong(6, req.maxOutputSize)
                                ups.setInt(7, existingId)
                                ups.executeUpdate()
                            }
                            scriptId = existingId
                            return@useDb
                        } else {
                            // Different author — reject
                            scriptId = -1  // sentinel for conflict
                            return@useDb
                        }
                    }
                }

                // No existing script — insert new
                conn.prepareStatement(
                    """
                    INSERT INTO scripts (name, description, author, code, code_size, language, save_result, max_output_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.name)
                    ps.setString(2, req.description)
                    ps.setString(3, req.author)
                    ps.setString(4, req.code)
                    ps.setInt(5, req.code.toByteArray().size)
                    ps.setString(6, req.language)
                    ps.setBoolean(7, req.saveResult)
                    ps.setLong(8, req.maxOutputSize)
                    val rs = ps.executeQuery()
                    if (rs.next()) scriptId = rs.getInt("id")
                }
            }

            if (scriptId == -1) {
                return HttpStatusCode.Conflict.description(
                    "Script '${req.name}' already exists with a different author. Cannot overwrite."
                )
            }
            return scriptId ?: HttpStatusCode.InternalServerError.description("Failed to create script")
        }
    }

    /** Get a script by ID. */
    object GetScript : AbstractPostRoute() {
        override val path: String = "/script/get"

        override suspend fun handle(ctx: RouteContext): Any? {
            val scriptId = ctx.receive<Map<String, Int>>()["scriptId"]
                ?: return HttpStatusCode.BadRequest.description("scriptId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var result: Map<String, Any?>? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT id, name, description, author, code, code_size, language, output, output_size, " +
                    "status, save_result, max_output_size, created_at, finished_at " +
                    "FROM scripts WHERE id = ?"
                ).use { ps ->
                    ps.setInt(1, scriptId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        result = mapOf(
                            "id" to rs.getInt("id"),
                            "name" to rs.getString("name"),
                            "description" to rs.getString("description"),
                            "author" to rs.getString("author"),
                            "code" to rs.getString("code"),
                            "codeSize" to rs.getInt("code_size"),
                            "language" to rs.getString("language"),
                            "output" to rs.getString("output"),
                            "outputSize" to rs.getInt("output_size"),
                            "status" to rs.getString("status"),
                            "saveResult" to rs.getBoolean("save_result"),
                            "maxOutputSize" to rs.getLong("max_output_size"),
                            "createdAt" to rs.getString("created_at"),
                            "finishedAt" to rs.getString("finished_at")
                        )
                    }
                }
            }
            return result ?: HttpStatusCode.NotFound.description("Script not found")
        }
    }

    /** List scripts with pagination. */
    object ListScripts : AbstractPostRoute() {
        override val path: String = "/script/list"

        data class Request(
            val limit: Int = 100,
            val offset: Int = 0
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val scripts = mutableListOf<Map<String, Any?>>()
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT id, name, description, author, code_size, language, status, " +
                    "save_result, max_output_size, created_at, finished_at " +
                    "FROM scripts ORDER BY created_at DESC LIMIT ? OFFSET ?"
                ).use { ps ->
                    ps.setInt(1, req.limit)
                    ps.setInt(2, req.offset)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        scripts.add(mapOf(
                            "id" to rs.getInt("id"),
                            "name" to rs.getString("name"),
                            "description" to rs.getString("description"),
                            "author" to rs.getString("author"),
                            "codeSize" to rs.getInt("code_size"),
                            "language" to rs.getString("language"),
                            "status" to rs.getString("status"),
                            "saveResult" to rs.getBoolean("save_result"),
                            "maxOutputSize" to rs.getLong("max_output_size"),
                            "createdAt" to rs.getString("created_at"),
                            "finishedAt" to rs.getString("finished_at")
                        ))
                    }
                }
            }
            return scripts
        }
    }

    /** Update a script's metadata or code. */
    object UpdateScript : AbstractPostRoute() {
        override val path: String = "/script/update"

        data class Request(
            val scriptId: Int,
            val name: String? = null,
            val description: String? = null,
            val code: String? = null,
            val language: String? = null,
            val saveResult: Boolean? = null,
            val maxOutputSize: Long? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    UPDATE scripts SET
                       name = COALESCE(?, name),
                       description = COALESCE(?, description),
                       code = COALESCE(?, code),
                       code_size = COALESCE(?, code_size),
                       language = COALESCE(?, language),
                       save_result = COALESCE(?, save_result),
                       max_output_size = LEAST(COALESCE(?, ?), ?)
                    WHERE id = ?
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.name)
                    ps.setString(2, req.description)
                    ps.setString(3, req.code)
                    ps.setInt(4, req.code?.toByteArray()?.size ?: 0)
                    ps.setString(5, req.language)
                    ps.setBoolean(6, req.saveResult ?: true)
                    ps.setLong(7, req.maxOutputSize ?: 0)
                    ps.setLong(8, 10737418240L) // MAX_OUTPUT_SIZE = 10GB
                    ps.setInt(9, req.scriptId)
                    ps.executeUpdate()
                }
            }
            return HttpStatusCode.OK
        }
    }

    /** Update script output (after execution). */
    object UpdateScriptOutput : AbstractPostRoute() {
        override val path: String = "/script/update_output"

        data class Request(
            val scriptId: Int,
            val output: String? = null,
            val status: String,
            val maxOutputSize: Long? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            fa.session.useDb { conn ->
                val truncatedOutput = if (req.maxOutputSize != null && req.output != null
                    && req.output.toByteArray().size > req.maxOutputSize) {
                    req.output.substring(0, req.maxOutputSize.toInt()) + "\n... (output truncated)"
                } else {
                    req.output
                }
                val outputSize = truncatedOutput?.toByteArray()?.size ?: 0

                conn.prepareStatement(
                    "UPDATE scripts SET output = ?, output_size = ?, status = ?, finished_at = ? WHERE id = ?"
                ).use { ps ->
                    ps.setString(1, truncatedOutput)
                    ps.setInt(2, outputSize)
                    ps.setString(3, req.status)
                    ps.setTimestamp(4, Timestamp.from(Instant.now()))
                    ps.setInt(5, req.scriptId)
                    ps.executeUpdate()
                }
            }
            return HttpStatusCode.OK
        }
    }

    /** Delete a script by ID. */
    object DeleteScript : AbstractPostRoute() {
        override val path: String = "/script/delete"

        override suspend fun handle(ctx: RouteContext): Any? {
            val scriptId = ctx.receive<Map<String, Int>>()["scriptId"]
                ?: return HttpStatusCode.BadRequest.description("scriptId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var deleted = false
            fa.session.useDb { conn ->
                conn.prepareStatement("DELETE FROM scripts WHERE id = ?").use { ps ->
                    ps.setInt(1, scriptId)
                    deleted = ps.executeUpdate() > 0
                }
            }
            return if (deleted) HttpStatusCode.OK else HttpStatusCode.NotFound.description("Script not found")
        }
    }

    // ============================================================
    //  Script Execution CRUD
    // ============================================================

    /** Create a script execution record. */
    object CreateExecution : AbstractPostRoute() {
        override val path: String = "/script/execution/create"

        data class Request(
            val scriptId: Int,
            val binaryId: Int? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var executionId: Int? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "INSERT INTO script_executions (script_id, binary_id) VALUES (?, ?) RETURNING id"
                ).use { ps ->
                    ps.setInt(1, req.scriptId)
                    if (req.binaryId != null) ps.setInt(2, req.binaryId) else ps.setNull(2, java.sql.Types.INTEGER)
                    val rs = ps.executeQuery()
                    if (rs.next()) executionId = rs.getInt("id")
                }
            }
            return executionId ?: HttpStatusCode.InternalServerError.description("Failed to create execution")
        }
    }

    /** Get a script execution by ID. */
    object GetExecution : AbstractPostRoute() {
        override val path: String = "/script/execution/get"

        override suspend fun handle(ctx: RouteContext): Any? {
            val executionId = ctx.receive<Map<String, Int>>()["executionId"]
                ?: return HttpStatusCode.BadRequest.description("executionId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var result: Map<String, Any?>? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT id, script_id, binary_id, status, output, error_message, " +
                    "started_at, finished_at FROM script_executions WHERE id = ?"
                ).use { ps ->
                    ps.setInt(1, executionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        val binaryId = rs.getInt("binary_id")
                        result = mapOf(
                            "id" to rs.getInt("id"),
                            "scriptId" to rs.getInt("script_id"),
                            "binaryId" to if (rs.wasNull()) null else binaryId,
                            "status" to rs.getString("status"),
                            "output" to rs.getString("output"),
                            "errorMessage" to rs.getString("error_message"),
                            "startedAt" to rs.getString("started_at"),
                            "finishedAt" to rs.getString("finished_at")
                        )
                    }
                }
            }
            return result ?: HttpStatusCode.NotFound.description("Execution not found")
        }
    }

    /** List executions for a script. */
    object ListExecutions : AbstractPostRoute() {
        override val path: String = "/script/execution/list"

        override suspend fun handle(ctx: RouteContext): Any? {
            val scriptId = ctx.receive<Map<String, Int>>()["scriptId"]
                ?: return HttpStatusCode.BadRequest.description("scriptId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val executions = mutableListOf<Map<String, Any?>>()
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT id, script_id, binary_id, status, output, error_message, " +
                    "started_at, finished_at FROM script_executions " +
                    "WHERE script_id = ? ORDER BY started_at DESC"
                ).use { ps ->
                    ps.setInt(1, scriptId)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        val binaryId = rs.getInt("binary_id")
                        executions.add(mapOf(
                            "id" to rs.getInt("id"),
                            "scriptId" to rs.getInt("script_id"),
                            "binaryId" to if (rs.wasNull()) null else binaryId,
                            "status" to rs.getString("status"),
                            "output" to rs.getString("output"),
                            "errorMessage" to rs.getString("error_message"),
                            "startedAt" to rs.getString("started_at"),
                            "finishedAt" to rs.getString("finished_at")
                        ))
                    }
                }
            }
            return executions
        }
    }

    /** Update a script execution's output and status. */
    object UpdateExecution : AbstractPostRoute() {
        override val path: String = "/script/execution/update"

        data class Request(
            val executionId: Int,
            val output: String? = null,
            val status: String,
            val errorMessage: String? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "UPDATE script_executions SET output = ?, status = ?, error_message = ?, finished_at = ? " +
                    "WHERE id = ?"
                ).use { ps ->
                    ps.setString(1, req.output)
                    ps.setString(2, req.status)
                    ps.setString(3, req.errorMessage)
                    ps.setTimestamp(4, Timestamp.from(Instant.now()))
                    ps.setInt(5, req.executionId)
                    ps.executeUpdate()
                }
            }
            return HttpStatusCode.OK
        }
    }

    /** Delete old executions. */
    object DeleteOldExecutions : AbstractPostRoute() {
        override val path: String = "/script/execution/delete_old"

        data class Request(val olderThanDays: Int = 30)

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            fa.session.useDb { conn ->
                // interval literal cannot be parameterized; validate input first
                val days = req.olderThanDays.coerceIn(1, 3650)
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(
                        "DELETE FROM script_executions WHERE started_at < now() - interval '$days days'"
                    )
                }
            }
            return HttpStatusCode.OK
        }
    }
}
