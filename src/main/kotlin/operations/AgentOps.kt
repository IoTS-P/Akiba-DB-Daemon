package org.iotsplab.akiba.dbDaemon.operations

import io.ktor.http.HttpStatusCode
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger
import org.iotsplab.akiba.dbDaemon.RouteContext
import org.iotsplab.akiba.dbDaemon.dbutil.defaultDbTypeAdapters
import org.postgresql.util.PGobject
import java.sql.ResultSet
import java.util.UUID

/**
 * Agent-related database operations for the Akiba Agent Framework.
 *
 * All routes follow the same pattern as [Modules] and [Queries]:
 *   1. Parse request body
 *   2. Authenticate via [fastAccess]
 *   3. Execute SQL via [UserDatabaseSession.useDb]
 *   4. Return result
 */
object AgentOps {

    // ============================================================
    //  Helper utilities
    // ============================================================

    /** Set a JSONB parameter on a PreparedStatement using PGobject. */
    private fun setJsonb(ps: java.sql.PreparedStatement, index: Int, json: String?) {
        if (json == null) {
            ps.setNull(index, java.sql.Types.OTHER)
        } else {
            val pgObj = PGobject()
            pgObj.type = "jsonb"
            pgObj.value = json
            ps.setObject(index, pgObj)
        }
    }

    /** Read a JSONB column from a ResultSet as a String (or null). */
    private fun ResultSet.getJsonb(columnLabel: String): String? =
        getObject(columnLabel)?.let { if (it is PGobject) it.value else it.toString() }

    /** Read a UUID column from a ResultSet as a String (or null). */
    private fun ResultSet.getUUIDStr(columnLabel: String): String? =
        getString(columnLabel)

    // ============================================================
    //  Session CRUD
    // ============================================================

    /** Create a new agent session. */
    object CreateSession : AbstractPostRoute() {
        override val path: String = "/agent/session/create"

        data class Request(
            val sessionName: String? = null,
            val binaryId: Int? = null,
            val moduleName: String? = null,
            val modelName: String? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var sessionId: String? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_sessions (session_name, binary_id, module_name, model_name)
                    VALUES (?, ?, ?, ?)
                    RETURNING session_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.sessionName)
                    if (req.binaryId != null) ps.setInt(2, req.binaryId) else ps.setNull(2, java.sql.Types.INTEGER)
                    ps.setString(3, req.moduleName)
                    ps.setString(4, req.modelName)
                    val rs = ps.executeQuery()
                    if (rs.next()) sessionId = rs.getUUIDStr("session_id")
                }
            }
            return sessionId ?: HttpStatusCode.InternalServerError.description("Failed to create session")
        }
    }

    /** Get a session by ID. */
    object GetSession : AbstractPostRoute() {
        override val path: String = "/agent/session/get"

        override suspend fun handle(ctx: RouteContext): Any? {
            val sessionId = ctx.receive<Map<String, String>>()["sessionId"]
                ?: return HttpStatusCode.BadRequest.description("sessionId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var result: Map<String, Any?>? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT session_id, session_name, status, binary_id, module_name, graph_id, " +
                    "model_name, created_at, updated_at, resumed_at, completed_at " +
                    "FROM agent_sessions WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, sessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        result = mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "sessionName" to rs.getString("session_name"),
                            "status" to rs.getString("status"),
                            "binaryId" to rs.getObject("binary_id"),
                            "moduleName" to rs.getString("module_name"),
                            "graphId" to rs.getUUIDStr("graph_id"),
                            "modelName" to rs.getString("model_name"),
                            "createdAt" to rs.getString("created_at"),
                            "updatedAt" to rs.getString("updated_at"),
                            "resumedAt" to rs.getString("resumed_at"),
                            "completedAt" to rs.getString("completed_at")
                        )
                    }
                }
            }
            return result ?: HttpStatusCode.NotFound.description("Session not found")
        }
    }

    /** List sessions with optional filters. */
    object ListSessions : AbstractPostRoute() {
        override val path: String = "/agent/session/list"

        data class Request(
            val status: String? = null,
            val binaryId: Int? = null,
            val moduleName: String? = null,
            val limit: Int = 50,
            val offset: Int = 0
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                // Build WHERE clause dynamically with parameterized values
                val conditions = mutableListOf<String>()
                val params = mutableListOf<Any?>()
                var paramIdx = 1

                req.status?.let {
                    conditions.add("status = ?")
                    params.add(it)
                }
                req.binaryId?.let {
                    conditions.add("binary_id = ?")
                    params.add(it)
                }
                req.moduleName?.let {
                    conditions.add("module_name = ?")
                    params.add(it)
                }

                val where = if (conditions.isEmpty()) "" else "WHERE ${conditions.joinToString(" AND ")}"
                val sql = "SELECT session_id, session_name, status, binary_id, module_name, " +
                    "model_name, created_at, updated_at FROM agent_sessions $where " +
                    "ORDER BY updated_at DESC LIMIT ? OFFSET ?"

                conn.prepareStatement(sql).use { ps ->
                    for (i in params.indices) {
                        val v = params[i]
                        when (v) {
                            is String -> ps.setString(paramIdx, v)
                            is Int -> ps.setInt(paramIdx, v)
                            else -> ps.setObject(paramIdx, v)
                        }
                        paramIdx++
                    }
                    ps.setInt(paramIdx++, req.limit.coerceAtMost(200))
                    ps.setInt(paramIdx, req.offset)

                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        results.add(mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "sessionName" to rs.getString("session_name"),
                            "status" to rs.getString("status"),
                            "binaryId" to rs.getObject("binary_id"),
                            "moduleName" to rs.getString("module_name"),
                            "modelName" to rs.getString("model_name"),
                            "createdAt" to rs.getString("created_at"),
                            "updatedAt" to rs.getString("updated_at")
                        ))
                    }
                }
            }
            return results
        }
    }

    /** Update session status / fields. */
    object UpdateSession : AbstractPostRoute() {
        override val path: String = "/agent/session/update"

        data class Request(
            val sessionId: String,
            val status: String? = null,
            val sessionName: String? = null,
            val graphId: String? = null,
            val modelName: String? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validStatuses = setOf("active", "suspended", "completed", "error")
            req.status?.let {
                if (it !in validStatuses)
                    return HttpStatusCode.BadRequest.description("Invalid status: $it. Must be one of $validStatuses")
            }

            val sets = mutableListOf<String>()
            val params = mutableListOf<Any?>()

            req.status?.let { sets.add("status = ?"); params.add(it) }
            req.sessionName?.let { sets.add("session_name = ?"); params.add(it) }
            req.graphId?.let { sets.add("graph_id = ?::uuid"); params.add(it) }
            req.modelName?.let { sets.add("model_name = ?"); params.add(it) }

            if (sets.isEmpty()) return HttpStatusCode.BadRequest.description("Nothing to update")

            // Auto-set timestamps based on status transitions
            req.status?.let { status ->
                when (status) {
                    "suspended" -> sets.add("resumed_at = NULL")
                    "completed", "error" -> sets.add("completed_at = now()")
                }
            }
            sets.add("updated_at = now()")

            val sql = "UPDATE agent_sessions SET ${sets.joinToString(", ")} WHERE session_id = ?::uuid"
            params.add(req.sessionId)

            var updated = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(sql).use { ps ->
                    var idx = 1
                    for (v in params) {
                        when (v) {
                            is String -> ps.setString(idx, v)
                            is Int -> ps.setInt(idx, v)
                            else -> ps.setObject(idx, v)
                        }
                        idx++
                    }
                    updated = ps.executeUpdate()
                }
            }
            return if (updated > 0) HttpStatusCode.OK.description("Session updated")
                   else HttpStatusCode.NotFound.description("Session not found")
        }
    }

    // ============================================================
    //  Messages (ChatMemory backing store)
    // ============================================================

    /** Append messages to a session. */
    object AppendMessages : AbstractPostRoute() {
        override val path: String = "/agent/message/append"

        data class MessageData(
            val role: String,
            val content: String? = null,
            val toolCallId: String? = null,
            val toolName: String? = null,
            val toolCallArgs: String? = null,   // JSON string
            val toolResult: String? = null,
            val tokenCount: Int? = null
        )

        data class Request(
            val sessionId: String,
            val messages: List<MessageData>
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validRoles = setOf("system", "user", "assistant", "tool")
            for (msg in req.messages) {
                if (msg.role !in validRoles)
                    return HttpStatusCode.BadRequest.description("Invalid role: ${msg.role}")
            }

            val ids: MutableList<Long> = mutableListOf()
            fa.session.useDb { conn ->
                // Get the current max message_index for this session
                var maxIndex = 0
                conn.prepareStatement(
                    "SELECT COALESCE(MAX(message_index), 0) FROM agent_messages WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) maxIndex = rs.getInt(1)
                }

                val insertSql = """
                    INSERT INTO agent_messages
                    (session_id, message_index, role, content, tool_call_id, tool_name, tool_call_args, tool_result, token_count)
                    VALUES (?::uuid, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                    RETURNING message_id
                """.trimIndent()

                conn.prepareStatement(insertSql).use { ps ->
                    for ((i, msg) in req.messages.withIndex()) {
                        ps.setString(1, req.sessionId)
                        ps.setInt(2, maxIndex + i + 1)
                        ps.setString(3, msg.role)
                        ps.setString(4, msg.content)
                        ps.setString(5, msg.toolCallId)
                        ps.setString(6, msg.toolName)
                        setJsonb(ps, 7, msg.toolCallArgs)
                        ps.setString(8, msg.toolResult)
                        if (msg.tokenCount != null) ps.setInt(9, msg.tokenCount) else ps.setNull(9, java.sql.Types.INTEGER)
                        ps.addBatch()
                    }
                    // Execute batch and collect IDs
                    val rs = ps.executeQuery()
                    while (rs.next()) ids.add(rs.getLong("message_id"))
                }
            }

            // Update session updated_at
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "UPDATE agent_sessions SET updated_at = now() WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    ps.executeUpdate()
                }
            }

            return ids
        }
    }

    /** Get messages for a session (with pagination). */
    object GetMessages : AbstractPostRoute() {
        override val path: String = "/agent/message/get"

        data class Request(
            val sessionId: String,
            val fromIndex: Int = 0,
            val limit: Int = 100
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT message_id, message_index, role, content, tool_call_id, tool_name, " +
                    "tool_call_args, tool_result, token_count, created_at " +
                    "FROM agent_messages WHERE session_id = ?::uuid AND message_index >= ? " +
                    "ORDER BY message_index ASC LIMIT ?"
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    ps.setInt(2, req.fromIndex)
                    ps.setInt(3, req.limit.coerceAtMost(500))
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        results.add(mapOf(
                            "messageId" to rs.getLong("message_id"),
                            "messageIndex" to rs.getInt("message_index"),
                            "role" to rs.getString("role"),
                            "content" to rs.getString("content"),
                            "toolCallId" to rs.getString("tool_call_id"),
                            "toolName" to rs.getString("tool_name"),
                            "toolCallArgs" to rs.getJsonb("tool_call_args"),
                            "toolResult" to rs.getString("tool_result"),
                            "tokenCount" to rs.getObject("token_count"),
                            "createdAt" to rs.getString("created_at")
                        ))
                    }
                }
            }
            return results
        }
    }

    /** Delete messages from a specific index onward (for trimming / sliding window). */
    object DeleteMessagesFrom : AbstractPostRoute() {
        override val path: String = "/agent/message/delete_from"

        data class Request(
            val sessionId: String,
            val fromIndex: Int
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var deleted = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "DELETE FROM agent_messages WHERE session_id = ?::uuid AND message_index >= ?"
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    ps.setInt(2, req.fromIndex)
                    deleted = ps.executeUpdate()
                }
            }
            return deleted
        }
    }

    // ============================================================
    //  Memories (cognitive layer)
    // ============================================================

    /** Store a new memory. */
    object StoreMemory : AbstractPostRoute() {
        override val path: String = "/agent/memory/store"

        data class Request(
            val sessionId: String? = null,
            val binaryId: Int? = null,
            val memoryType: String = "finding",
            val scope: String = "session",
            val key: String? = null,
            val content: String,
            val importance: Double? = null,
            val metadata: String? = null    // JSON string
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var memoryId: Long? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_memories
                    (session_id, binary_id, memory_type, scope, key, content, importance, metadata)
                    VALUES (?::uuid, ?, ?, ?, ?, ?, ?, ?::jsonb)
                    RETURNING memory_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    if (req.binaryId != null) ps.setInt(2, req.binaryId) else ps.setNull(2, java.sql.Types.INTEGER)
                    ps.setString(3, req.memoryType)
                    ps.setString(4, req.scope)
                    ps.setString(5, req.key)
                    ps.setString(6, req.content)
                    if (req.importance != null) ps.setDouble(7, req.importance) else ps.setNull(7, java.sql.Types.FLOAT)
                    setJsonb(ps, 8, req.metadata)
                    val rs = ps.executeQuery()
                    if (rs.next()) memoryId = rs.getLong("memory_id")
                }
            }
            return memoryId ?: HttpStatusCode.InternalServerError.description("Failed to store memory")
        }
    }

    /** Query memories by various filters. */
    object QueryMemories : AbstractPostRoute() {
        override val path: String = "/agent/memory/query"

        data class Request(
            val sessionId: String? = null,
            val binaryId: Int? = null,
            val memoryType: String? = null,
            val scope: String? = null,
            val key: String? = null,
            val minImportance: Double? = null,
            val limit: Int = 50
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                val conditions = mutableListOf<String>()
                val params = mutableListOf<Any?>()

                req.sessionId?.let { conditions.add("session_id = ?::uuid"); params.add(it) }
                req.binaryId?.let { conditions.add("binary_id = ?"); params.add(it) }
                req.memoryType?.let { conditions.add("memory_type = ?"); params.add(it) }
                req.scope?.let { conditions.add("scope = ?"); params.add(it) }
                req.key?.let { conditions.add("key = ?"); params.add(it) }
                req.minImportance?.let { conditions.add("importance >= ?"); params.add(it) }

                val where = if (conditions.isEmpty()) "" else "WHERE ${conditions.joinToString(" AND ")}"
                val sql = "SELECT memory_id, session_id, binary_id, memory_type, scope, key, " +
                    "content, importance, metadata, created_at FROM agent_memories $where " +
                    "ORDER BY importance DESC NULLS LAST, created_at DESC LIMIT ?"

                conn.prepareStatement(sql).use { ps ->
                    var idx = 1
                    for (v in params) {
                        when (v) {
                            is String -> ps.setString(idx, v)
                            is Int -> ps.setInt(idx, v)
                            is Double -> ps.setDouble(idx, v)
                            else -> ps.setObject(idx, v)
                        }
                        idx++
                    }
                    ps.setInt(idx, req.limit.coerceAtMost(200))

                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        results.add(mapOf(
                            "memoryId" to rs.getLong("memory_id"),
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "binaryId" to rs.getObject("binary_id"),
                            "memoryType" to rs.getString("memory_type"),
                            "scope" to rs.getString("scope"),
                            "key" to rs.getString("key"),
                            "content" to rs.getString("content"),
                            "importance" to rs.getObject("importance"),
                            "metadata" to rs.getJsonb("metadata"),
                            "createdAt" to rs.getString("created_at")
                        ))
                    }
                }
            }
            return results
        }
    }

    // ============================================================
    //  Session Context
    // ============================================================

    /** Save or update session context (environment + config + module_data). */
    object SaveContext : AbstractPostRoute() {
        override val path: String = "/agent/context/save"

        data class Request(
            val sessionId: String,
            val environment: String? = null,    // JSON string
            val contextConfig: String? = null,   // JSON string
            val moduleData: String? = null       // JSON string
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            // Build SET clause dynamically
            val sets = mutableListOf<String>()
            val jsonParams = mutableListOf<Pair<Int, String?>>()

            req.environment?.let { sets.add("environment = ?::jsonb"); jsonParams.add(Pair(1, it)) }
            req.contextConfig?.let { sets.add("context_config = ?::jsonb"); jsonParams.add(Pair(2, it)) }
            req.moduleData?.let { sets.add("module_data = ?::jsonb"); jsonParams.add(Pair(3, it)) }

            if (sets.isEmpty()) return HttpStatusCode.BadRequest.description("Nothing to save")

            sets.add("updated_at = now()")

            val sql = """
                INSERT INTO agent_session_contexts (session_id, environment, context_config, module_data, updated_at)
                VALUES (?::uuid, ?::jsonb, ?::jsonb, ?::jsonb, now())
                ON CONFLICT (session_id) DO UPDATE SET ${sets.joinToString(", ")}
            """.trimIndent()

            var updated = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(sql).use { ps ->
                    // INSERT values
                    ps.setString(1, req.sessionId)
                    setJsonb(ps, 2, req.environment ?: "{}")
                    setJsonb(ps, 3, req.contextConfig ?: "{}")
                    setJsonb(ps, 4, req.moduleData ?: "{}")

                    // ON CONFLICT UPDATE values — map to correct positions
                    // The SET clause params come after the INSERT params
                    var idx = 5
                    for ((_, jsonStr) in jsonParams.sortedBy { it.first }) {
                        setJsonb(ps, idx, jsonStr)
                        idx++
                    }
                    // updated_at = now() has no param

                    updated = ps.executeUpdate()
                }
            }
            return if (updated > 0) HttpStatusCode.OK.description("Context saved")
                   else HttpStatusCode.InternalServerError.description("Failed to save context")
        }
    }

    /** Load session context. */
    object LoadContext : AbstractPostRoute() {
        override val path: String = "/agent/context/load"

        override suspend fun handle(ctx: RouteContext): Any? {
            val sessionId = ctx.receive<Map<String, String>>()["sessionId"]
                ?: return HttpStatusCode.BadRequest.description("sessionId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var result: Map<String, Any?>? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT session_id, environment, context_config, module_data, updated_at " +
                    "FROM agent_session_contexts WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, sessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        result = mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "environment" to rs.getJsonb("environment"),
                            "contextConfig" to rs.getJsonb("context_config"),
                            "moduleData" to rs.getJsonb("module_data"),
                            "updatedAt" to rs.getString("updated_at")
                        )
                    }
                }
            }
            return result ?: HttpStatusCode.NotFound.description("Context not found")
        }
    }

    // ============================================================
    //  Graph Topology
    // ============================================================

    /** Create a new agent graph. */
    object CreateGraph : AbstractPostRoute() {
        override val path: String = "/agent/graph/create"

        data class Request(
            val sessionId: String,
            val graphName: String? = null,
            val entryNode: String? = null,
            val maxIterations: Int = 10,
            val convergenceThreshold: Double = 0.95,
            val cycleStrategy: String = "MAX_ITERATIONS"
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validStrategies = setOf("MAX_ITERATIONS", "CONVERGENCE", "MANUAL")
            if (req.cycleStrategy !in validStrategies)
                return HttpStatusCode.BadRequest.description("Invalid cycleStrategy: ${req.cycleStrategy}")

            var graphId: String? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_graphs
                    (session_id, graph_name, entry_node, max_iterations, convergence_threshold, cycle_strategy)
                    VALUES (?::uuid, ?, ?, ?, ?, ?)
                    RETURNING graph_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    ps.setString(2, req.graphName)
                    ps.setString(3, req.entryNode)
                    ps.setInt(4, req.maxIterations)
                    ps.setDouble(5, req.convergenceThreshold)
                    ps.setString(6, req.cycleStrategy)
                    val rs = ps.executeQuery()
                    if (rs.next()) graphId = rs.getUUIDStr("graph_id")
                }

                // Bind graph to session
                conn.prepareStatement(
                    "UPDATE agent_sessions SET graph_id = ?::uuid WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, graphId)
                    ps.setString(2, req.sessionId)
                    ps.executeUpdate()
                }
            }
            return graphId ?: HttpStatusCode.InternalServerError.description("Failed to create graph")
        }
    }

    /** Add a node to a graph. */
    object AddGraphNode : AbstractPostRoute() {
        override val path: String = "/agent/graph/add_node"

        data class Request(
            val graphId: String,
            val nodeId: String,
            val nodeName: String? = null,
            val role: String = "WORKER",
            val modelName: String? = null,
            val systemPrompt: String? = null,
            val tools: String = "[]",         // JSON array string
            val config: String = "{}"         // JSON object string
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validRoles = setOf("SUPERVISOR", "WORKER", "CRITIC", "SYNTHESIZER", "GATEKEEPER", "CUSTOM")
            if (req.role !in validRoles)
                return HttpStatusCode.BadRequest.description("Invalid role: ${req.role}")

            var inserted = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_graph_nodes
                    (graph_id, node_id, node_name, role, model_name, system_prompt, tools, config)
                    VALUES (?::uuid, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.graphId)
                    ps.setString(2, req.nodeId)
                    ps.setString(3, req.nodeName)
                    ps.setString(4, req.role)
                    ps.setString(5, req.modelName)
                    ps.setString(6, req.systemPrompt)
                    setJsonb(ps, 7, req.tools)
                    setJsonb(ps, 8, req.config)
                    inserted = ps.executeUpdate()
                }
            }
            return if (inserted > 0) HttpStatusCode.OK.description("Node added")
                   else HttpStatusCode.InternalServerError.description("Failed to add node")
        }
    }

    /** Add an edge to a graph. */
    object AddGraphEdge : AbstractPostRoute() {
        override val path: String = "/agent/graph/add_edge"

        data class Request(
            val graphId: String,
            val sourceNode: String,
            val targetNode: String,
            val edgeType: String = "DELEGATE",
            val condition: String? = null,
            val priority: Int = 0,
            val config: String = "{}"         // JSON object string
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validEdgeTypes = setOf("DELEGATE", "REPORT", "COLLABORATE", "CRITIQUE", "FEEDBACK", "BROADCAST", "GATE")
            if (req.edgeType !in validEdgeTypes)
                return HttpStatusCode.BadRequest.description("Invalid edgeType: ${req.edgeType}")

            var edgeId: Long? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_graph_edges
                    (graph_id, source_node, target_node, edge_type, condition, priority, config)
                    VALUES (?::uuid, ?, ?, ?, ?, ?, ?::jsonb)
                    RETURNING edge_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.graphId)
                    ps.setString(2, req.sourceNode)
                    ps.setString(3, req.targetNode)
                    ps.setString(4, req.edgeType)
                    ps.setString(5, req.condition)
                    ps.setInt(6, req.priority)
                    setJsonb(ps, 7, req.config)
                    val rs = ps.executeQuery()
                    if (rs.next()) edgeId = rs.getLong("edge_id")
                }
            }
            return edgeId ?: HttpStatusCode.InternalServerError.description("Failed to add edge")
        }
    }

    /** Load the full graph definition (graph + nodes + edges). */
    object LoadGraph : AbstractPostRoute() {
        override val path: String = "/agent/graph/load"

        override suspend fun handle(ctx: RouteContext): Any? {
            val graphId = ctx.receive<Map<String, String>>()["graphId"]
                ?: return HttpStatusCode.BadRequest.description("graphId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var graphInfo: Map<String, Any?>? = null
            val nodes: MutableList<Map<String, Any?>> = mutableListOf()
            val edges: MutableList<Map<String, Any?>> = mutableListOf()

            fa.session.useDb { conn ->
                // Load graph metadata
                conn.prepareStatement(
                    "SELECT graph_id, session_id, graph_name, entry_node, max_iterations, " +
                    "convergence_threshold, cycle_strategy, created_at FROM agent_graphs WHERE graph_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, graphId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        graphInfo = mapOf(
                            "graphId" to rs.getUUIDStr("graph_id"),
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "graphName" to rs.getString("graph_name"),
                            "entryNode" to rs.getString("entry_node"),
                            "maxIterations" to rs.getInt("max_iterations"),
                            "convergenceThreshold" to rs.getDouble("convergence_threshold"),
                            "cycleStrategy" to rs.getString("cycle_strategy"),
                            "createdAt" to rs.getString("created_at")
                        )
                    }
                }

                // Load nodes
                conn.prepareStatement(
                    "SELECT graph_id, node_id, node_name, role, model_name, system_prompt, tools, config " +
                    "FROM agent_graph_nodes WHERE graph_id = ?::uuid ORDER BY node_id"
                ).use { ps ->
                    ps.setString(1, graphId)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        nodes.add(mapOf(
                            "graphId" to rs.getUUIDStr("graph_id"),
                            "nodeId" to rs.getString("node_id"),
                            "nodeName" to rs.getString("node_name"),
                            "role" to rs.getString("role"),
                            "modelName" to rs.getString("model_name"),
                            "systemPrompt" to rs.getString("system_prompt"),
                            "tools" to rs.getJsonb("tools"),
                            "config" to rs.getJsonb("config")
                        ))
                    }
                }

                // Load edges
                conn.prepareStatement(
                    "SELECT graph_id, edge_id, source_node, target_node, edge_type, condition, priority, config " +
                    "FROM agent_graph_edges WHERE graph_id = ?::uuid ORDER BY edge_id"
                ).use { ps ->
                    ps.setString(1, graphId)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        edges.add(mapOf(
                            "graphId" to rs.getUUIDStr("graph_id"),
                            "edgeId" to rs.getLong("edge_id"),
                            "sourceNode" to rs.getString("source_node"),
                            "targetNode" to rs.getString("target_node"),
                            "edgeType" to rs.getString("edge_type"),
                            "condition" to rs.getString("condition"),
                            "priority" to rs.getInt("priority"),
                            "config" to rs.getJsonb("config")
                        ))
                    }
                }
            }

            return if (graphInfo != null) {
                mapOf("graph" to graphInfo, "nodes" to nodes, "edges" to edges)
            } else {
                HttpStatusCode.NotFound.description("Graph not found")
            }
        }
    }

    // ============================================================
    //  Graph Execution Log
    // ============================================================

    /** Record a graph execution step. */
    object RecordExecution : AbstractPostRoute() {
        override val path: String = "/agent/execution/record"

        data class Request(
            val graphId: String,
            val sessionId: String,
            val iteration: Int,
            val nodeId: String,
            val inputSummary: String? = null,
            val outputSummary: String? = null,
            val status: String = "running",
            val durationMs: Long? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validStatuses = setOf("running", "completed", "failed", "skipped")
            if (req.status !in validStatuses)
                return HttpStatusCode.BadRequest.description("Invalid status: ${req.status}")

            var executionId: Long? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_graph_executions
                    (graph_id, session_id, iteration, node_id, input_summary, output_summary, status, duration_ms)
                    VALUES (?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?)
                    RETURNING execution_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.graphId)
                    ps.setString(2, req.sessionId)
                    ps.setInt(3, req.iteration)
                    ps.setString(4, req.nodeId)
                    ps.setString(5, req.inputSummary)
                    ps.setString(6, req.outputSummary)
                    ps.setString(7, req.status)
                    if (req.durationMs != null) ps.setLong(8, req.durationMs) else ps.setNull(8, java.sql.Types.BIGINT)
                    val rs = ps.executeQuery()
                    if (rs.next()) executionId = rs.getLong("execution_id")
                }
            }
            return executionId ?: HttpStatusCode.InternalServerError.description("Failed to record execution")
        }
    }

    /** Get execution log for a session. */
    object GetExecutions : AbstractPostRoute() {
        override val path: String = "/agent/execution/get"

        data class Request(
            val sessionId: String,
            val graphId: String? = null,
            val limit: Int = 100
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                val sql = if (req.graphId != null) {
                    "SELECT execution_id, graph_id, session_id, iteration, node_id, input_summary, " +
                    "output_summary, status, duration_ms, created_at FROM agent_graph_executions " +
                    "WHERE session_id = ?::uuid AND graph_id = ?::uuid ORDER BY iteration, execution_id LIMIT ?"
                } else {
                    "SELECT execution_id, graph_id, session_id, iteration, node_id, input_summary, " +
                    "output_summary, status, duration_ms, created_at FROM agent_graph_executions " +
                    "WHERE session_id = ?::uuid ORDER BY iteration, execution_id LIMIT ?"
                }

                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, req.sessionId)
                    if (req.graphId != null) {
                        ps.setString(2, req.graphId)
                        ps.setInt(3, req.limit.coerceAtMost(500))
                    } else {
                        ps.setInt(2, req.limit.coerceAtMost(500))
                    }
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        results.add(mapOf(
                            "executionId" to rs.getLong("execution_id"),
                            "graphId" to rs.getUUIDStr("graph_id"),
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "iteration" to rs.getInt("iteration"),
                            "nodeId" to rs.getString("node_id"),
                            "inputSummary" to rs.getString("input_summary"),
                            "outputSummary" to rs.getString("output_summary"),
                            "status" to rs.getString("status"),
                            "durationMs" to rs.getObject("duration_ms"),
                            "createdAt" to rs.getString("created_at")
                        ))
                    }
                }
            }
            return results
        }
    }

    // ============================================================
    //  Human-in-the-Loop
    // ============================================================

    /** Create a human input request. */
    object RequestHumanInput : AbstractPostRoute() {
        override val path: String = "/agent/human/request"

        data class Request(
            val sessionId: String,
            val requestText: String,
            val expiresAt: String? = null    // ISO timestamp
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var inputId: Long? = null
            fa.session.useDb { conn ->
                val sql = if (req.expiresAt != null) {
                    """
                    INSERT INTO agent_human_inputs (session_id, request_text, expires_at)
                    VALUES (?::uuid, ?, ?::timestamptz)
                    RETURNING input_id
                    """.trimIndent()
                } else {
                    """
                    INSERT INTO agent_human_inputs (session_id, request_text)
                    VALUES (?::uuid, ?)
                    RETURNING input_id
                    """.trimIndent()
                }

                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, req.sessionId)
                    ps.setString(2, req.requestText)
                    if (req.expiresAt != null) ps.setString(3, req.expiresAt)
                    val rs = ps.executeQuery()
                    if (rs.next()) inputId = rs.getLong("input_id")
                }
            }
            return inputId ?: HttpStatusCode.InternalServerError.description("Failed to create human input request")
        }
    }

    /** Respond to a human input request. */
    object RespondHumanInput : AbstractPostRoute() {
        override val path: String = "/agent/human/respond"

        data class Request(
            val inputId: Long,
            val responseText: String,
            val status: String = "answered"   // answered | cancelled
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validStatuses = setOf("answered", "cancelled")
            if (req.status !in validStatuses)
                return HttpStatusCode.BadRequest.description("Invalid status: ${req.status}")

            var updated = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    UPDATE agent_human_inputs
                    SET response_text = ?, status = ?, answered_at = now()
                    WHERE input_id = ? AND status = 'pending'
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.responseText)
                    ps.setString(2, req.status)
                    ps.setLong(3, req.inputId)
                    updated = ps.executeUpdate()
                }
            }
            return if (updated > 0) HttpStatusCode.OK.description("Response recorded")
                   else HttpStatusCode.NotFound.description("Pending input request not found")
        }
    }

    /** Poll pending human input requests for a session. */
    object PollHumanInputs : AbstractPostRoute() {
        override val path: String = "/agent/human/poll"

        data class Request(
            val sessionId: String,
            val includeAnswered: Boolean = false
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                val sql = if (req.includeAnswered) {
                    "SELECT input_id, session_id, request_text, response_text, status, " +
                    "created_at, answered_at, expires_at FROM agent_human_inputs " +
                    "WHERE session_id = ?::uuid ORDER BY created_at DESC"
                } else {
                    "SELECT input_id, session_id, request_text, response_text, status, " +
                    "created_at, answered_at, expires_at FROM agent_human_inputs " +
                    "WHERE session_id = ?::uuid AND status = 'pending' ORDER BY created_at"
                }

                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, req.sessionId)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        results.add(mapOf(
                            "inputId" to rs.getLong("input_id"),
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "requestText" to rs.getString("request_text"),
                            "responseText" to rs.getString("response_text"),
                            "status" to rs.getString("status"),
                            "createdAt" to rs.getString("created_at"),
                            "answeredAt" to rs.getString("answered_at"),
                            "expiresAt" to rs.getString("expires_at")
                        ))
                    }
                }
            }
            return results
        }
    }

    // ============================================================
    //  Tool Call Audit
    // ============================================================

    /** Record a tool call for auditing. */
    object RecordToolCall : AbstractPostRoute() {
        override val path: String = "/agent/tool_call/record"

        data class Request(
            val sessionId: String,
            val messageId: Long? = null,
            val nodeId: String? = null,
            val toolName: String,
            val toolArgs: String? = null,        // JSON string
            val resultSummary: String? = null,
            val success: Boolean = true,
            val durationMs: Long? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var callId: Long? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_tool_calls
                    (session_id, message_id, node_id, tool_name, tool_args, result_summary, success, duration_ms)
                    VALUES (?::uuid, ?, ?, ?, ?::jsonb, ?, ?, ?)
                    RETURNING call_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    if (req.messageId != null) ps.setLong(2, req.messageId) else ps.setNull(2, java.sql.Types.BIGINT)
                    ps.setString(3, req.nodeId)
                    ps.setString(4, req.toolName)
                    setJsonb(ps, 5, req.toolArgs)
                    ps.setString(6, req.resultSummary)
                    ps.setBoolean(7, req.success)
                    if (req.durationMs != null) ps.setLong(8, req.durationMs) else ps.setNull(8, java.sql.Types.BIGINT)
                    val rs = ps.executeQuery()
                    if (rs.next()) callId = rs.getLong("call_id")
                }
            }
            return callId ?: HttpStatusCode.InternalServerError.description("Failed to record tool call")
        }
    }
}
