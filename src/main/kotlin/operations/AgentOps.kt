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

    private val runtimeStates = setOf("running", "standby", "msghandle", "cancelling", "closed", "error")

    /** Normalize legacy UI statuses onto the runtime_state enum. */
    private fun normalizeRuntimeState(raw: String?): String? = when (raw?.lowercase()) {
        null -> null
        "running" -> "running"
        "active" -> "running"
        "standby" -> "standby"
        "suspended" -> "standby"
        "msghandle" -> "msghandle"
        "cancelling" -> "cancelling"
        "closed" -> "closed"
        "completed" -> "closed"
        "cancelled" -> "closed"
        "error" -> "error"
        "failed" -> "error"
        else -> null
    }

    private fun ResultSet.sessionRuntimeState(): String =
        getString("runtime_state") ?: normalizeRuntimeState(getString("status")) ?: "running"

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
            val modelName: String? = null,
            val projectName: String? = null,
            /**
             * Optional UUID of the parent session. Used by `spawn_sub_agent`
             * to attach the spawned child to the parent's tree.
             */
            val parentSessionId: String? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            // Validate that the requested parent actually exists (if specified).
            // We allow null (top-level session).
            if (req.parentSessionId != null) {
                var parentExists = false
                fa.session.useDb { conn ->
                    conn.prepareStatement(
                        "SELECT 1 FROM agent_sessions WHERE session_id = ?::uuid"
                    ).use { ps ->
                        ps.setString(1, req.parentSessionId)
                        val rs = ps.executeQuery()
                        if (rs.next()) parentExists = true
                    }
                }
                if (!parentExists)
                    return HttpStatusCode.BadRequest.description(
                        "Parent session '${req.parentSessionId}' does not exist"
                    )
            }

            var sessionId: String? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_sessions
                        (session_name, binary_id, module_name, model_name, project_name, parent_session_id)
                    VALUES (?, ?, ?, ?, ?, ?::uuid)
                    RETURNING session_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.sessionName)
                    if (req.binaryId != null) ps.setInt(2, req.binaryId) else ps.setNull(2, java.sql.Types.INTEGER)
                    ps.setString(3, req.moduleName)
                    ps.setString(4, req.modelName)
                    ps.setString(5, req.projectName)
                    if (req.parentSessionId != null) ps.setString(6, req.parentSessionId)
                    else ps.setNull(6, java.sql.Types.OTHER)
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
                    "model_name, project_name, created_at, updated_at, resumed_at, completed_at, transcript, " +
                    "parent_session_id, lifecycle, runtime_state, closing_reason " +
                    "FROM agent_sessions WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, sessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        result = mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "sessionName" to rs.getString("session_name"),
                            "status" to rs.sessionRuntimeState(),
                            "binaryId" to rs.getObject("binary_id"),
                            "moduleName" to rs.getString("module_name"),
                            "graphId" to rs.getUUIDStr("graph_id"),
                            "modelName" to rs.getString("model_name"),
                            "createdAt" to rs.getString("created_at"),
                            "updatedAt" to rs.getString("updated_at"),
                            "resumedAt" to rs.getString("resumed_at"),
                            "completedAt" to rs.getString("completed_at"),
                            "transcript" to rs.getString("transcript"),
                            "parentSessionId" to rs.getUUIDStr("parent_session_id"),
                            "lifecycle" to rs.getString("lifecycle"),
                            "runtimeState" to rs.getString("runtime_state"),
                            "closingReason" to rs.getString("closing_reason"),
                        )
                    }
                }
            }
            return result ?: HttpStatusCode.NotFound.description("Session not found")
        }
    }

    /**
     * List direct children of a parent session. Used by the frontend to
     * walk the parent → child tree for a multi-agent session.
     */
    object GetSessionChildren : AbstractPostRoute() {
        override val path: String = "/agent/session/children"

        override suspend fun handle(ctx: RouteContext): Any? {
            val body = ctx.receive<Map<String, String>>()
            val parentId = body["parentSessionId"]
                ?: return HttpStatusCode.BadRequest.description("parentSessionId is required")
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                val sql = """
                    SELECT session_id, session_name, status, runtime_state, binary_id, module_name,
                           model_name, project_name, created_at, updated_at, parent_session_id
                    FROM agent_sessions
                    WHERE parent_session_id = ?::uuid
                    ORDER BY created_at ASC
                """.trimIndent()
                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, parentId)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        val runtimeState = rs.sessionRuntimeState()
                        results.add(mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "sessionName" to rs.getString("session_name"),
                            "status" to runtimeState,
                            "runtimeState" to runtimeState,
                            "binaryId" to rs.getObject("binary_id"),
                            "moduleName" to rs.getString("module_name"),
                            "modelName" to rs.getString("model_name"),
                            "projectName" to rs.getString("project_name"),
                            "createdAt" to rs.getString("created_at"),
                            "updatedAt" to rs.getString("updated_at"),
                            "parentSessionId" to rs.getUUIDStr("parent_session_id")
                        ))
                    }
                }
            }
            return results
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
            val offset: Int = 0,
            /**
             * Filter by parent_session_id.
             *   - `null` (default): only top-level sessions (parent_session_id IS NULL)
             *   - a specific UUID: only direct children of that parent
             *   - the literal string "ALL" (or any non-UUID string): return every session
             *     regardless of parent (used by advanced views)
             */
            val parentSessionId: String? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            // Normalize the status filter up-front so we can return
            // a clean 400 before opening the DB connection (and
            // without escaping from `useDb`'s non-inline lambda).
            val normalizedStatus: String? = req.status?.let { raw ->
                normalizeRuntimeState(raw)
                    ?: return HttpStatusCode.BadRequest.description(
                        "Invalid status: $raw. Must be one of $runtimeStates"
                    )
            }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                // Build WHERE clause dynamically with parameterized values
                val conditions = mutableListOf<String>()
                val params = mutableListOf<Any?>()
                var paramIdx = 1

                if (normalizedStatus != null) {
                    conditions.add("runtime_state = ?")
                    params.add(normalizedStatus)
                }
                req.binaryId?.let {
                    conditions.add("binary_id = ?")
                    params.add(it)
                }
                req.moduleName?.let {
                    conditions.add("module_name = ?")
                    params.add(it)
                }
                when (val pid = req.parentSessionId) {
                    null -> {
                        // Default: only top-level sessions
                        conditions.add("parent_session_id IS NULL")
                    }
                    "ALL" -> {
                        // Explicit opt-in: return every session
                    }
                    else -> {
                        conditions.add("parent_session_id = ?::uuid")
                        params.add(pid)
                    }
                }

                val where = if (conditions.isEmpty()) "" else "WHERE ${conditions.joinToString(" AND ")}"
                val sql = "SELECT session_id, session_name, status, runtime_state, binary_id, module_name, " +
                    "model_name, project_name, created_at, updated_at, parent_session_id " +
                    "FROM agent_sessions $where " +
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
                        val runtimeState = rs.sessionRuntimeState()
                        results.add(mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "sessionName" to rs.getString("session_name"),
                            "status" to runtimeState,
                            "runtimeState" to runtimeState,
                            "binaryId" to rs.getObject("binary_id"),
                            "moduleName" to rs.getString("module_name"),
                            "modelName" to rs.getString("model_name"),
                            "projectName" to rs.getString("project_name"),
                            "createdAt" to rs.getString("created_at"),
                            "updatedAt" to rs.getString("updated_at"),
                            "parentSessionId" to rs.getUUIDStr("parent_session_id")
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
            val modelName: String? = null,
            val transcript: String? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            req.status?.let {
                if (it !in runtimeStates)
                    return HttpStatusCode.BadRequest.description(
                        "Invalid status: $it. Must be one of $runtimeStates"
                    )
            }

            val sets = mutableListOf<String>()
            val params = mutableListOf<Any?>()

            req.status?.let { sets.add("status = ?"); params.add(it) }
            req.sessionName?.let { sets.add("session_name = ?"); params.add(it) }
            req.graphId?.let { sets.add("graph_id = ?::uuid"); params.add(it) }
            req.modelName?.let { sets.add("model_name = ?"); params.add(it) }
            req.transcript?.let { sets.add("transcript = ?"); params.add(it) }

            if (sets.isEmpty()) return HttpStatusCode.BadRequest.description("Nothing to update")

            // Auto-set timestamps based on runtime_state transitions.
            // The new enum mirrors runtime_state; `closed` and `error`
            // are terminal and stamp completed_at, `standby` clears
            // resumed_at (it represents a fresh park).
            req.status?.let { status ->
                when (status) {
                    "standby" -> sets.add("resumed_at = NULL")
                    "closed", "error" -> sets.add("completed_at = now()")
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

    /**
     * Set the session's `lifecycle`. The framework sets this once at
     * agent creation; downstream transitions stay out of this route
     * so the orchestrator remains the single source of truth for the
     * session state machine.
     */
    object SetSessionLifecycle : AbstractPostRoute() {
        override val path: String = "/agent/session/set_lifecycle"

        data class Request(
            val sessionId: String,
            val lifecycle: String,                  // one_shot | standby
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            if (req.lifecycle !in setOf("one_shot", "standby"))
                return HttpStatusCode.BadRequest.description(
                    "Invalid lifecycle: ${req.lifecycle}. Must be one_shot or standby"
                )

            var updated = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "UPDATE agent_sessions SET lifecycle = ?::text, updated_at = now() " +
                        "WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, req.lifecycle)
                    ps.setString(2, req.sessionId)
                    updated = ps.executeUpdate()
                }
            }
            return if (updated > 0) HttpStatusCode.OK.description("Lifecycle set to ${req.lifecycle}")
                   else HttpStatusCode.NotFound.description("Session not found")
        }
    }

    /**
     * Update the session's `runtime_state` and optional
     * `closing_reason`. The route performs no transition validation —
     * the framework's [org.iotsplab.akiba.llm.agent.RuntimeState] is
     * the source of truth for legal transitions; this route just
     * mirrors them. The schema CHECK constraint rejects unknown
     * values, so a typo at the call site fails loud.
     *
     * When [closingReason] is non-null and [runtimeState] is
     * `closed`, the daemon also stamps `completed_at = now()` so
     * finished sessions are queryable by their end timestamp.
     */
    object SetRuntimeState : AbstractPostRoute() {
        override val path: String = "/agent/session/set_runtime_state"

        data class Request(
            val sessionId: String,
            val runtimeState: String,             // running | standby | msghandle | cancelling | closed | error
            val closingReason: String? = null,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validStates = setOf("running", "standby", "msghandle", "cancelling", "closed", "error")
            if (req.runtimeState !in validStates)
                return HttpStatusCode.BadRequest.description(
                    "Invalid runtimeState: ${req.runtimeState}. Must be one of $validStates"
                )

            var updated = 0
            fa.session.useDb { conn ->
                val sets = mutableListOf("runtime_state = ?::text", "updated_at = now()")
                val params = mutableListOf<Any?>(req.runtimeState)
                if (req.closingReason != null) {
                    sets.add("closing_reason = ?")
                    params.add(req.closingReason)
                }
                if (req.runtimeState == "closed") {
                    sets.add("completed_at = COALESCE(completed_at, now())")
                }
                params.add(req.sessionId)
                val sql = "UPDATE agent_sessions SET ${sets.joinToString(", ")} " +
                    "WHERE session_id = ?::uuid"
                conn.prepareStatement(sql).use { ps ->
                    var idx = 1
                    for (v in params) {
                        when (v) {
                            is String -> ps.setString(idx, v)
                            else -> ps.setObject(idx, v)
                        }
                        idx++
                    }
                    updated = ps.executeUpdate()
                }
            }
            return if (updated > 0) HttpStatusCode.OK.description(
                "Runtime state set to ${req.runtimeState}" +
                    if (req.closingReason != null) " (reason=${req.closingReason})" else ""
            ) else HttpStatusCode.NotFound.description("Session not found")
        }
    }

    /**
     * Fetch the session's current `runtime_state` and
     * `closing_reason`. Cheap query used by the dispatcher
     * pre-flight and by [org.iotsplab.akiba.llm.agent.JobHandle.await]
     * to resolve the initial state without joining the framework's
     * in-process cache.
     */
    object GetRuntimeState : AbstractPostRoute() {
        override val path: String = "/agent/session/get_runtime_state"

        data class Request(val sessionId: String)

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var row: Map<String, Any?>? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT session_id, runtime_state, closing_reason, lifecycle, status " +
                        "FROM agent_sessions WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        val runtimeState = rs.sessionRuntimeState()
                        row = mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "runtimeState" to runtimeState,
                            "closingReason" to rs.getString("closing_reason"),
                            "lifecycle" to rs.getString("lifecycle"),
                            "status" to runtimeState,
                        )
                    }
                }
            }
            return row ?: HttpStatusCode.NotFound.description("Session not found")
        }
    }

    /**
     * List all non-closed descendants of a root session. Used by the
     * cascade cancel walker and the OrphanReaper. One SQL round-trip
     * — recursion is emulated in SQL via a recursive CTE so the
     * daemon can answer the question without a Kotlin-side BFS.
     */
    object ListLiveSubtree : AbstractPostRoute() {
        override val path: String = "/agent/session/list_live_subtree"

        data class Request(
            val rootSessionId: String,
            val includeClosed: Boolean = false,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            fa.session.useDb { conn ->
                val filter = if (req.includeClosed) "" else "AND runtime_state <> 'closed'"
                val sql = """
                    WITH RECURSIVE subtree AS (
                        SELECT session_id, parent_session_id, runtime_state, closing_reason,
                               lifecycle, status
                        FROM agent_sessions
                        WHERE session_id = ?::uuid
                        UNION ALL
                        SELECT s.session_id, s.parent_session_id, s.runtime_state, s.closing_reason,
                               s.lifecycle, s.status
                        FROM agent_sessions s
                        JOIN subtree st ON s.parent_session_id = st.session_id
                        WHERE s.session_id <> st.session_id
                    )
                    SELECT session_id, parent_session_id, runtime_state, closing_reason,
                           lifecycle, status
                    FROM subtree
                    WHERE 1=1 $filter
                    ORDER BY session_id
                """.trimIndent()
                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, req.rootSessionId)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        val runtimeState = rs.sessionRuntimeState()
                        results.add(mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "parentSessionId" to rs.getUUIDStr("parent_session_id"),
                            "runtimeState" to runtimeState,
                            "closingReason" to rs.getString("closing_reason"),
                            "lifecycle" to rs.getString("lifecycle"),
                            "status" to runtimeState,
                        ))
                    }
                }
            }
            return results
        }
    }

    // ============================================================
    //  Agent status snapshot (used by get_agent_status tool)
    // ============================================================
    //
    // One-row snapshot of a single target session's runtime state
    // plus the aggregated counters the calling LLM needs to
    // reason about itself (e.g. "am I a standby agent that
    // should emit the `Enter standby mode.` marker?") or a
    // child (e.g. "is this standby child still running?"):
    //
    //   * lastMessageAt        — the wall-clock time of the most
    //                            recent row in agent_messages for
    //                            that session (NULL when no
    //                            messages yet).
    //   * totalInputTokens     — sum of input_token_count over all
    //                            assistant messages.  NULL/0 when
    //                            the LLM provider did not report
    //                            input tokens.
    //   * totalOutputTokens    — sum of token_count over all
    //                            assistant messages.
    //   * totalToolCalls       — count of rows in agent_tool_calls
    //                            for the session.
    //   * childCount           — number of direct children
    //                            (parent_session_id = target).
    //   * runningChildCount    — subset of childCount whose
    //                            runtime_state is 'running' or
    //                            'msghandle'.
    //
    // ## Access policy (auto-detected)
    //
    // The caller does NOT pass a `scope` field.  The route
    // reads the target's `parent_session_id` and the caller's
    // `parent_session_id` and derives the relationship
    // automatically:
    //
    //   * "self"          — target == caller
    //   * "direct_child"  — target.parent == caller
    //   * "direct_parent" — caller.parent == target
    //   * "sibling"       — target.parent == caller.parent (and
    //                       neither is null and target != caller)
    //   * "other"         — anything else (grandchild, uncle,
    //                       unrelated session, etc.)
    //
    // Currently only `self` and `direct_child` are admitted
    // (the two cases the LLM legitimately needs: "look at my
    // own state" and "look at my immediate child").  The other
    // three are rejected with `status='forbidden'` and a hint
    // so the LLM knows the relationship was detected but
    // access is not granted yet.  Adding `direct_parent` /
    // `sibling` access will require an explicit permission
    // model and is intentionally deferred — keeping the deny
    // path here is the safer default.
    //
    // childLimit is a hard cap (default 64, hard max 256) on
    // the directChildren array.  When the cap is hit the
    // response sets `directChildrenTruncated=true`.
    object GetAgentStatus : AbstractPostRoute() {
        override val path: String = "/agent/session/agent_status"

        data class Request(
            val callerSessionId: String,
            val targetSessionId: String,
            /** Hard cap on returned direct children. Default 64, max 256. */
            val childLimit: Int = 64,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            // ---- Validate input ---------------------------------------
            if (req.callerSessionId.isBlank()) {
                return HttpStatusCode.BadRequest.description("callerSessionId is required")
            }
            if (req.targetSessionId.isBlank()) {
                return HttpStatusCode.BadRequest.description("targetSessionId is required")
            }
            val cap = req.childLimit.coerceIn(1, 256)

            // ---- Load target + per-target counters --------------------
            var snapshot: Map<String, Any?>? = null
            var callerParent: String? = null
            var callerExists: Boolean = req.callerSessionId == req.targetSessionId
            fa.session.useDb { conn ->
                // Two single-row reads: the target and the caller.
                // We need the caller's parent_session_id to detect
                // direct_parent and sibling relationships.  The
                // target read pulls the same aggregated counters as
                // before.
                val targetSql = """
                    SELECT
                        s.session_id,
                        s.session_name,
                        s.runtime_state,
                        s.lifecycle,
                        s.parent_session_id,
                        s.binary_id,
                        s.module_name,
                        s.model_name,
                        s.closing_reason,
                        s.created_at,
                        s.updated_at,
                        s.completed_at,
                        (SELECT MAX(m.created_at)
                           FROM agent_messages m
                          WHERE m.session_id = s.session_id) AS last_message_at,
                        COALESCE((SELECT SUM(m.input_token_count)
                                    FROM agent_messages m
                                   WHERE m.session_id = s.session_id
                                     AND m.input_token_count IS NOT NULL), 0) AS total_input_tokens,
                        COALESCE((SELECT SUM(m.token_count)
                                    FROM agent_messages m
                                   WHERE m.session_id = s.session_id
                                     AND m.token_count IS NOT NULL), 0) AS total_output_tokens,
                        (SELECT COUNT(*)
                           FROM agent_tool_calls t
                          WHERE t.session_id = s.session_id) AS total_tool_calls,
                        (SELECT COUNT(*)
                           FROM agent_sessions c
                          WHERE c.parent_session_id = s.session_id) AS child_count,
                        (SELECT COUNT(*)
                           FROM agent_sessions c
                          WHERE c.parent_session_id = s.session_id
                            AND c.runtime_state IN ('running','msghandle')) AS running_child_count
                    FROM agent_sessions s
                    WHERE s.session_id = ?::uuid
                """.trimIndent()
                conn.prepareStatement(targetSql).use { ps ->
                    ps.setString(1, req.targetSessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        val runtimeState = rs.sessionRuntimeState()
                        snapshot = mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "sessionName" to rs.getString("session_name"),
                            "runtimeState" to runtimeState,
                            "lifecycle" to rs.getString("lifecycle"),
                            "parentSessionId" to rs.getUUIDStr("parent_session_id"),
                            "binaryId" to rs.getObject("binary_id"),
                            "moduleName" to rs.getString("module_name"),
                            "modelName" to rs.getString("model_name"),
                            "closingReason" to rs.getString("closing_reason"),
                            "createdAt" to rs.getString("created_at"),
                            "updatedAt" to rs.getString("updated_at"),
                            "completedAt" to rs.getString("completed_at"),
                            "lastMessageAt" to rs.getString("last_message_at"),
                            "totalInputTokens" to (rs.getLong("total_input_tokens").takeIf { !rs.wasNull() } ?: 0L),
                            "totalOutputTokens" to (rs.getLong("total_output_tokens").takeIf { !rs.wasNull() } ?: 0L),
                            "totalToolCalls" to rs.getLong("total_tool_calls"),
                            "childCount" to rs.getLong("child_count"),
                            "runningChildCount" to rs.getLong("running_child_count"),
                        )
                    }
                }
                if (!callerExists) {
                    conn.prepareStatement(
                        "SELECT parent_session_id FROM agent_sessions " +
                            "WHERE session_id = ?::uuid"
                    ).use { ps ->
                        ps.setString(1, req.callerSessionId)
                        val rs = ps.executeQuery()
                        if (rs.next()) {
                            callerParent = rs.getString("parent_session_id")?.takeIf { it.isNotBlank() }
                            callerExists = true
                        }
                    }
                }
            }
            if (snapshot == null) {
                return HttpStatusCode.NotFound.description(
                    "Target session '${req.targetSessionId}' not found"
                )
            }
            if (!callerExists) {
                // Caller row not found is logged at warn level by
                // the framework (the agent_session row is created
                // before the agent starts); treat as 403 so the
                // LLM gets a structured denial.
                return mapOf(
                    "status" to "forbidden",
                    "error" to "callerSessionId '${req.callerSessionId}' not found",
                    "callerSessionId" to req.callerSessionId,
                    "targetSessionId" to req.targetSessionId,
                    "relationship" to "other",
                    "hint" to "The calling session is no longer registered. " +
                        "This usually means the parent process restarted without " +
                        "re-attaching to its child sessions.",
                )
            }
            val targetParent = (snapshot["parentSessionId"] as String?)?.takeIf { it.isNotBlank() }

            // ---- Auto-detect relationship ----------------------------
            val relationship: String = when {
                req.targetSessionId == req.callerSessionId -> "self"
                targetParent != null && targetParent == req.callerSessionId -> "direct_child"
                callerParent != null && callerParent == req.targetSessionId -> "direct_parent"
                targetParent != null && targetParent == callerParent -> "sibling"
                else -> "other"
            }

            // ---- Enforce access policy -------------------------------
            // Default visibility: `self` and `direct_child` only.
            // `direct_parent` / `sibling` / `other` are detected
            // (so the LLM can see WHY access was denied) but
            // currently rejected.  Future permission models can
            // hook in here to admit additional relationships
            // without changing the wire format.
            val accessGranted: Boolean
            val denyReason: String?
            when (relationship) {
                "self" -> {
                    accessGranted = true
                    denyReason = null
                }
                "direct_child" -> {
                    accessGranted = true
                    denyReason = null
                }
                else -> {
                    accessGranted = false
                    denyReason = when (relationship) {
                        "direct_parent" ->
                            "target is the direct parent of caller; viewing a parent " +
                                "session's state is not yet permitted by the default policy"
                        "sibling" ->
                            "target is a sibling of caller (shares the same parent); " +
                                "cross-sibling reads are not yet permitted by the default policy"
                        else ->
                            "target is neither the caller nor a direct child of caller; " +
                                "the default policy only permits self / direct_child reads"
                    }
                }
            }
            if (!accessGranted) {
                return mapOf(
                    "status" to "forbidden",
                    "error" to denyReason!!,
                    "callerSessionId" to req.callerSessionId,
                    "targetSessionId" to req.targetSessionId,
                    "relationship" to relationship,
                    "hint" to "Default visibility is 'self' and 'direct_child' only. " +
                        "The relationship between caller and target was auto-detected " +
                        "as '$relationship' and is currently not admitted. " +
                        "Cross-relationship reads will be enabled after a future " +
                        "permission model is added.",
                )
            }

            // ---- Direct children of the target (capped) ---------------
            val children = mutableListOf<Map<String, Any?>>()
            var truncated = false
            fa.session.useDb { conn ->
                val sql = """
                    SELECT session_id, session_name, runtime_state, lifecycle, parent_session_id,
                           binary_id, module_name, model_name, created_at, updated_at
                      FROM agent_sessions
                     WHERE parent_session_id = ?::uuid
                     ORDER BY created_at ASC
                     LIMIT ?
                """.trimIndent()
                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, req.targetSessionId)
                    ps.setInt(2, cap + 1)  // +1 sentinel so we can detect truncation
                    val rs = ps.executeQuery()
                    var n = 0
                    while (rs.next()) {
                        n++
                        if (n > cap) { truncated = true; break }
                        val state = rs.sessionRuntimeState()
                        children.add(mapOf(
                            "sessionId" to rs.getUUIDStr("session_id"),
                            "sessionName" to rs.getString("session_name"),
                            "runtimeState" to state,
                            "lifecycle" to rs.getString("lifecycle"),
                            "parentSessionId" to rs.getUUIDStr("parent_session_id"),
                            "binaryId" to rs.getObject("binary_id"),
                            "moduleName" to rs.getString("module_name"),
                            "modelName" to rs.getString("model_name"),
                            "createdAt" to rs.getString("created_at"),
                            "updatedAt" to rs.getString("updated_at"),
                        ))
                    }
                }
            }

            return mapOf(
                "status" to "ok",
                "relationship" to relationship,
                "callerSessionId" to req.callerSessionId,
                "target" to snapshot,
                "directChildren" to children,
                "directChildrenTruncated" to truncated,
            )
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
            val tokenCount: Int? = null,
            val inputTokenCount: Int? = null
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
                    (session_id, message_index, role, content, tool_call_id, tool_name, tool_call_args, tool_result, token_count, input_token_count)
                    VALUES (?::uuid, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                    RETURNING message_id
                """.trimIndent()

                conn.prepareStatement(insertSql, java.sql.Statement.RETURN_GENERATED_KEYS).use { ps ->
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
                        if (msg.inputTokenCount != null) ps.setInt(10, msg.inputTokenCount) else ps.setNull(10, java.sql.Types.INTEGER)
                        ps.addBatch()
                    }
                    // Execute batch and collect generated keys
                    ps.executeBatch()
                    val rs = ps.generatedKeys
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
                    "tool_call_args, tool_result, token_count, input_token_count, created_at " +
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
                            "inputTokenCount" to rs.getObject("input_token_count"),
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
            val toolCallId: String? = null,
            val toolName: String,
            val toolArgs: String? = null,        // JSON string
            val resultUuid: String? = null,
            val resultSummary: String? = null,
            val resultContent: String? = null,
            val resultOriginalBytes: Int? = null,
            val resultStoredBytes: Int? = null,
            val resultTruncated: Boolean? = null,
            val resultSha256: String? = null,
            val storagePolicy: String? = null,
            val success: Boolean = true,
            val durationMs: Long? = null
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val resultUuid = req.resultUuid?.takeIf { it.isNotBlank() } ?: UUID.randomUUID().toString()
            var callId: Long? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_tool_calls
                    (session_id, message_id, node_id, tool_call_id, tool_name, tool_args, result_uuid,
                     result_summary, result_original_bytes, result_stored_bytes, result_truncated,
                     result_sha256, storage_policy, success, duration_ms)
                    VALUES (?::uuid, ?, ?, ?, ?, ?::jsonb, ?::uuid, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING call_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    if (req.messageId != null) ps.setLong(2, req.messageId) else ps.setNull(2, java.sql.Types.BIGINT)
                    ps.setString(3, req.nodeId)
                    ps.setString(4, req.toolCallId)
                    ps.setString(5, req.toolName)
                    setJsonb(ps, 6, req.toolArgs)
                    ps.setString(7, resultUuid)
                    ps.setString(8, req.resultSummary)
                    if (req.resultOriginalBytes != null) ps.setInt(9, req.resultOriginalBytes) else ps.setNull(9, java.sql.Types.INTEGER)
                    if (req.resultStoredBytes != null) ps.setInt(10, req.resultStoredBytes) else ps.setNull(10, java.sql.Types.INTEGER)
                    if (req.resultTruncated != null) ps.setBoolean(11, req.resultTruncated) else ps.setNull(11, java.sql.Types.BOOLEAN)
                    ps.setString(12, req.resultSha256)
                    ps.setString(13, req.storagePolicy)
                    ps.setBoolean(14, req.success)
                    if (req.durationMs != null) ps.setLong(15, req.durationMs) else ps.setNull(15, java.sql.Types.BIGINT)
                    val rs = ps.executeQuery()
                    if (rs.next()) callId = rs.getLong("call_id")
                }

                if (req.resultContent != null && callId != null) {
                    conn.prepareStatement(
                        """
                        INSERT INTO agent_tool_call_results
                        (result_uuid, call_id, session_id, message_id, tool_call_id, tool_name, tool_args,
                         content, original_bytes, stored_bytes, truncated, sha256, storage_policy)
                        VALUES (?::uuid, ?, ?::uuid, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (result_uuid) DO NOTHING
                        """.trimIndent()
                    ).use { ps ->
                        ps.setString(1, resultUuid)
                        ps.setLong(2, callId)
                        ps.setString(3, req.sessionId)
                        if (req.messageId != null) ps.setLong(4, req.messageId) else ps.setNull(4, java.sql.Types.BIGINT)
                        ps.setString(5, req.toolCallId)
                        ps.setString(6, req.toolName)
                        setJsonb(ps, 7, req.toolArgs)
                        ps.setString(8, req.resultContent)
                        ps.setInt(9, req.resultOriginalBytes ?: req.resultContent.toByteArray(Charsets.UTF_8).size)
                        ps.setInt(10, req.resultStoredBytes ?: req.resultContent.toByteArray(Charsets.UTF_8).size)
                        ps.setBoolean(11, req.resultTruncated ?: false)
                        ps.setString(12, req.resultSha256)
                        ps.setString(13, req.storagePolicy ?: "full")
                        ps.executeUpdate()
                    }
                }
            }
            return callId ?: HttpStatusCode.InternalServerError.description("Failed to record tool call")
        }
    }

    /** Find stored tool call results by session and optional fingerprints. */
    object FindToolCallResults : AbstractPostRoute() {
        override val path: String = "/agent/tool_call/result/find"

        data class Request(
            val sessionId: String,
            val resultSha256: String? = null,
            val toolName: String? = null,
            val toolArgs: String? = null,
            val limit: Int = 20
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }
            val safeLimit = req.limit.coerceIn(1, 100)
            val results: MutableList<Map<String, Any?>> = mutableListOf()

            fa.session.useDb { conn ->
                val conditions = mutableListOf("session_id = ?::uuid")
                if (!req.resultSha256.isNullOrBlank()) conditions.add("sha256 = ?")
                if (!req.toolName.isNullOrBlank()) conditions.add("tool_name = ?")
                if (!req.toolArgs.isNullOrBlank()) conditions.add("tool_args = ?::jsonb")

                val sql = """
                    SELECT result_uuid, call_id, session_id, message_id, tool_call_id, tool_name, tool_args,
                           original_bytes, stored_bytes, truncated, sha256, storage_policy, created_at
                    FROM agent_tool_call_results
                    WHERE ${conditions.joinToString(" AND ")}
                    ORDER BY created_at ASC
                    LIMIT ?
                """.trimIndent()

                conn.prepareStatement(sql).use { ps ->
                    var idx = 1
                    ps.setString(idx++, req.sessionId)
                    if (!req.resultSha256.isNullOrBlank()) ps.setString(idx++, req.resultSha256)
                    if (!req.toolName.isNullOrBlank()) ps.setString(idx++, req.toolName)
                    if (!req.toolArgs.isNullOrBlank()) setJsonb(ps, idx++, req.toolArgs)
                    ps.setInt(idx, safeLimit)

                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        results.add(mapOf(
                            "resultUuid" to rs.getString("result_uuid"),
                            "callId" to rs.getLong("call_id"),
                            "sessionId" to rs.getString("session_id"),
                            "messageId" to rs.getObject("message_id"),
                            "toolCallId" to rs.getString("tool_call_id"),
                            "toolName" to rs.getString("tool_name"),
                            "toolArgs" to rs.getJsonb("tool_args"),
                            "originalBytes" to rs.getInt("original_bytes"),
                            "storedBytes" to rs.getInt("stored_bytes"),
                            "truncated" to rs.getBoolean("truncated"),
                            "sha256" to rs.getString("sha256"),
                            "storagePolicy" to rs.getString("storage_policy"),
                            "createdAt" to rs.getString("created_at")
                        ))
                    }
                }
            }
            return results
        }
    }

    /** Retrieve a stored tool call result by UUID. */
    object GetToolCallResult : AbstractPostRoute() {
        override val path: String = "/agent/tool_call/result/get"

        data class Request(
            val resultUuid: String,
            val offset: Int = 0,
            val limit: Int = 8000,
            val grep: String? = null,
            val around: Int = 3
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }
            val safeLimit = req.limit.coerceIn(1, 8000)
            val safeOffset = req.offset.coerceAtLeast(0)
            var result: Map<String, Any?>? = null

            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    SELECT result_uuid, call_id, session_id, message_id, tool_call_id, tool_name, tool_args,
                           content, original_bytes, stored_bytes, truncated, sha256, storage_policy, created_at
                    FROM agent_tool_call_results
                    WHERE result_uuid = ?::uuid
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.resultUuid)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        val content = rs.getString("content") ?: ""
                        val slice = if (!req.grep.isNullOrBlank()) {
                            grepSnippet(content, req.grep, req.around, safeLimit)
                        } else {
                            content.drop(safeOffset).take(safeLimit)
                        }
                        result = mapOf(
                            "resultUuid" to rs.getString("result_uuid"),
                            "callId" to rs.getLong("call_id"),
                            "sessionId" to rs.getString("session_id"),
                            "messageId" to rs.getObject("message_id"),
                            "toolCallId" to rs.getString("tool_call_id"),
                            "toolName" to rs.getString("tool_name"),
                            "toolArgs" to rs.getJsonb("tool_args"),
                            "content" to slice,
                            "offset" to safeOffset,
                            "returnedChars" to slice.length,
                            "storedChars" to content.length,
                            "originalBytes" to rs.getInt("original_bytes"),
                            "storedBytes" to rs.getInt("stored_bytes"),
                            "truncated" to rs.getBoolean("truncated"),
                            "sha256" to rs.getString("sha256"),
                            "storagePolicy" to rs.getString("storage_policy"),
                            "createdAt" to rs.getString("created_at")
                        )
                    }
                }
            }
            return result ?: HttpStatusCode.NotFound.description("Tool call result not found")
        }

        private fun grepSnippet(content: String, grep: String, around: Int, limit: Int): String {
            val lines = content.lines()
            val idx = lines.indexOfFirst { it.contains(grep, ignoreCase = true) }
            if (idx < 0) return "No match for '$grep'."
            val start = (idx - around.coerceAtLeast(0)).coerceAtLeast(0)
            val end = (idx + around.coerceAtLeast(0) + 1).coerceAtMost(lines.size)
            return lines.subList(start, end).joinToString("\n").take(limit)
        }
    }

    // ============================================================
    //  Transcript Append
    // ============================================================

    /** Append Markdown content to a session's transcript field. */
    object AppendTranscript : AbstractPostRoute() {
        override val path: String = "/agent/transcript/append"

        data class Request(
            val sessionId: String,
            val content: String
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var updated = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "UPDATE agent_sessions SET transcript = COALESCE(transcript, '') || ?, updated_at = now() " +
                    "WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, req.content)
                    ps.setString(2, req.sessionId)
                    updated = ps.executeUpdate()
                }
            }
            return if (updated > 0) HttpStatusCode.OK.description("Content appended")
                   else HttpStatusCode.NotFound.description("Session not found")
        }
    }
}
