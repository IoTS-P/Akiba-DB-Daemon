package org.iotsplab.akiba.dbDaemon.operations

import io.ktor.http.HttpStatusCode
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.RouteContext
import java.sql.ResultSet
import java.util.UUID

/**
 * Mailbox routes for inter-agent asynchronous messages. The schema is
 * intentionally narrow (sender/recipient/kind/body/optional artifact/reply
 * pointers); heavy payloads travel through `agent_artifacts` and are
 * referenced by id.
 *
 * Lifecycle access control is enforced in
 * [org.iotsplab.akiba.llm.agent.AgentMailboxService.send] and re-checked
 * here as defense-in-depth.
 */
object MailboxOps {

    /**
     * Well-known UUID for the reserved "system" sender used by
     * [org.iotsplab.akiba.llm.agent.AgentMailboxDispatcher] when
     * delivering synthetic wake-condition messages.  The row is
     * created lazily by [SendMessage] on first use so the
     * `sender_session_id` FK on `agent_mailbox_messages` is
     * satisfied.
     */
    private const val SYSTEM_SESSION_UUID = "00000000-0000-0000-0000-000000000000"

    // ============================================================
    //  Row mapping
    // ============================================================

    private fun ResultSet.toMap(): Map<String, Any?> = mapOf(
        "messageId" to getLong("message_id"),
        "senderSessionId" to getString("sender_session_id"),
        "recipientSessionId" to getString("recipient_session_id"),
        "kind" to getString("kind"),
        "subject" to getString("subject"),
        "body" to getString("body"),
        "relatedArtifactId" to getObject("related_artifact_id"),
        "inReplyToMessageId" to getObject("in_reply_to_message_id"),
        "priority" to getInt("priority"),
        "readAt" to getString("read_at"),
        "ackedAt" to getString("acked_at"),
        "createdAt" to getString("created_at"),
    )

    // ============================================================
    //  send_agent_message
    // ============================================================

    /**
     * Insert a new mailbox message.
     *
     * Mailbox access policy (enforced by the framework, re-checked here):
     * sender must exist and not be terminal; recipient must exist; a
     * recipient with `lifecycle='one_shot'` and terminal `status` is
     * rejected — its history is readable through `query_session_history`
     * / `read_history_tool_call` but it cannot be re-targeted.
     */
    object SendMessage : AbstractPostRoute() {
        override val path: String = "/agent/mailbox/send"

        data class Request(
            val senderSessionId: String,
            val recipientSessionId: String,
            val kind: String = "note",
            val subject: String? = null,
            val body: String,
            val relatedArtifactId: Long? = null,
            val inReplyToMessageId: Long? = null,
            val priority: Int = 0,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validKinds = setOf("note", "request", "reply", "cancel", "heartbeat")
            if (req.kind !in validKinds)
                return HttpStatusCode.BadRequest.description(
                    "Invalid kind: ${req.kind}. Must be one of $validKinds"
                )
            if (req.body.isBlank())
                return HttpStatusCode.BadRequest.description("body must not be blank")

            // Translate the reserved "system" sender to the well-known
            // system session UUID.  The framework's wake-condition
            // dispatcher uses "system" for synthetic wake messages that
            // have no real sender session; the UUID is required to
            // satisfy the sender_session_id FK + NOT-NULL constraint.
            val isSystemSender = req.senderSessionId == "system"
            val senderSessionId = if (isSystemSender) SYSTEM_SESSION_UUID else req.senderSessionId

            if (senderSessionId == req.recipientSessionId)
                return HttpStatusCode.BadRequest.description(
                    "Sender and recipient must differ"
                )

            // Verify both sessions exist and inspect recipient's lifecycle.
            var senderOk = false
            var senderTerminal = false
            var recipientOk = false
            var recipientLifecycle: String? = null
            var recipientStatus: String? = null
            fa.session.useDb { conn ->
                if (isSystemSender) {
                    // Lazily create the system session so the FK on
                    // sender_session_id is satisfied.  ON CONFLICT makes
                    // this idempotent — a no-op after the first call.
                    conn.prepareStatement(
                        """INSERT INTO agent_sessions
                               (session_id, session_name, status, lifecycle, runtime_state)
                           VALUES (?::uuid, 'system', 'running', 'one_shot', 'running')
                           ON CONFLICT (session_id) DO NOTHING"""
                    ).use { ps ->
                        ps.setString(1, SYSTEM_SESSION_UUID)
                        ps.executeUpdate()
                    }
                    senderOk = true
                    senderTerminal = false
                } else {
                    conn.prepareStatement(
                        "SELECT status FROM agent_sessions WHERE session_id = ?::uuid"
                    ).use { ps ->
                        ps.setString(1, senderSessionId)
                        val rs = ps.executeQuery()
                        if (rs.next()) {
                            senderOk = true
                            val s = rs.getString("status")
                            senderTerminal = s == "completed" || s == "error"
                        }
                    }
                }
                conn.prepareStatement(
                    "SELECT lifecycle, status FROM agent_sessions WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, req.recipientSessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        recipientOk = true
                        recipientLifecycle = rs.getString("lifecycle")
                        recipientStatus = rs.getString("status")
                    }
                }
            }

            if (!senderOk)
                return HttpStatusCode.NotFound.description(
                    "Sender session '$senderSessionId' not found"
                )
            if (!recipientOk)
                return HttpStatusCode.NotFound.description(
                    "Recipient session '${req.recipientSessionId}' not found"
                )
            if (senderTerminal)
                return HttpStatusCode.Forbidden.description(
                    "Sender session '$senderSessionId' is terminal ($recipientStatus); cannot send"
                )

            // Lifecycle access control: one-shot sessions are terminal by design.
            if (recipientLifecycle == "one_shot" &&
                (recipientStatus == "completed" || recipientStatus == "error")
            ) {
                return HttpStatusCode.Forbidden.description(
                    "Recipient '${req.recipientSessionId}' is one-shot and terminal ($recipientStatus); " +
                        "its history is readable via query_session_history but it cannot be re-targeted by mailbox messages"
                )
            }

            var messageId: Long? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_mailbox_messages
                        (sender_session_id, recipient_session_id, kind, subject, body,
                         related_artifact_id, in_reply_to_message_id, priority)
                    VALUES (?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?)
                    RETURNING message_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, senderSessionId)
                    ps.setString(2, req.recipientSessionId)
                    ps.setString(3, req.kind)
                    ps.setString(4, req.subject)
                    ps.setString(5, req.body)
                    if (req.relatedArtifactId != null) ps.setLong(6, req.relatedArtifactId) else ps.setNull(6, java.sql.Types.BIGINT)
                    if (req.inReplyToMessageId != null) ps.setLong(7, req.inReplyToMessageId) else ps.setNull(7, java.sql.Types.BIGINT)
                    ps.setInt(8, req.priority)
                    val rs = ps.executeQuery()
                    if (rs.next()) messageId = rs.getLong("message_id")
                }
            }
            return messageId ?: HttpStatusCode.InternalServerError.description("Failed to send mailbox message")
        }
    }

    // ============================================================
    //  list_agent_messages (peek — does NOT mark read)
    // ============================================================

    object ListMessages : AbstractPostRoute() {
        override val path: String = "/agent/mailbox/list"

        data class Request(
            val sessionId: String,
            val limit: Int = 50,
            val includeRead: Boolean = false,
            /** When true, return only messages that have been read
             *  (read_at IS NOT NULL) but NOT yet acked (acked_at IS NULL).
             *  This is the "pending" set — items the agent has seen in a
             *  previous wake but hasn't handled yet.  Takes precedence
             *  over [includeRead] when both are set. */
            val onlyPending: Boolean = false,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val results: MutableList<Map<String, Any?>> = mutableListOf()
            val safeLimit = req.limit.coerceIn(1, 500)

            val readFilter = when {
                req.onlyPending -> "AND read_at IS NOT NULL AND acked_at IS NULL"
                !req.includeRead -> "AND read_at IS NULL"
                else -> ""
            }

            fa.session.useDb { conn ->
                val sql = """
                    SELECT message_id, sender_session_id, recipient_session_id, kind, subject, body,
                           related_artifact_id, in_reply_to_message_id, priority,
                           read_at, acked_at, created_at
                    FROM agent_mailbox_messages
                    WHERE recipient_session_id = ?::uuid
                      $readFilter
                    ORDER BY priority DESC, created_at ASC
                    LIMIT ?
                """.trimIndent()
                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, req.sessionId)
                    ps.setInt(2, safeLimit)
                    val rs = ps.executeQuery()
                    while (rs.next()) results.add(rs.toMap())
                }
            }
            return results
        }
    }

    // ============================================================
    //  drain_agent_messages (peek + mark read)
    // ============================================================

    object DrainMessages : AbstractPostRoute() {
        override val path: String = "/agent/mailbox/drain"

        data class Request(
            val sessionId: String,
            val limit: Int = 50,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val safeLimit = req.limit.coerceIn(1, 500)
            val drained: MutableList<Map<String, Any?>> = mutableListOf()
            val drainedIds = mutableListOf<Long>()

            fa.session.useDb { conn ->
                // SELECT … FOR UPDATE SKIP LOCKED so concurrent drains on
                // the same recipient don't double-mark the same row.
                conn.prepareStatement(
                    """
                    SELECT message_id, sender_session_id, recipient_session_id, kind, subject, body,
                           related_artifact_id, in_reply_to_message_id, priority,
                           read_at, acked_at, created_at
                    FROM agent_mailbox_messages
                    WHERE recipient_session_id = ?::uuid AND read_at IS NULL
                    ORDER BY priority DESC, created_at ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    ps.setInt(2, safeLimit)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        drained.add(rs.toMap())
                        drainedIds.add(rs.getLong("message_id"))
                    }
                }
                if (drainedIds.isNotEmpty()) {
                    val placeholders = drainedIds.joinToString(",") { "?" }
                    conn.prepareStatement(
                        "UPDATE agent_mailbox_messages SET read_at = now() " +
                            "WHERE message_id IN ($placeholders)"
                    ).use { ps ->
                        drainedIds.forEachIndexed { i, id -> ps.setLong(i + 1, id) }
                        ps.executeUpdate()
                    }
                }
            }
            return mapOf("drained" to drainedIds.size, "messages" to drained)
        }
    }

    // ============================================================
    //  ack_agent_message
    // ============================================================

    object AckMessage : AbstractPostRoute() {
        override val path: String = "/agent/mailbox/ack"

        data class Request(
            val sessionId: String,
            val messageId: Long,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var updated = 0
            var alreadyAcked = false
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    UPDATE agent_mailbox_messages
                    SET acked_at = now()
                    WHERE message_id = ? AND recipient_session_id = ?::uuid AND acked_at IS NULL
                    """.trimIndent()
                ).use { ps ->
                    ps.setLong(1, req.messageId)
                    ps.setString(2, req.sessionId)
                    updated = ps.executeUpdate()
                }
                // If the UPDATE matched 0 rows, check whether the message
                // exists and is already acked.  If so, this is an
                // idempotent re-ack — return OK instead of 404 so the
                // caller doesn't treat an already-handled message as an
                // error and get stuck retrying.
                if (updated == 0) {
                    conn.prepareStatement(
                        """
                        SELECT 1 FROM agent_mailbox_messages
                        WHERE message_id = ? AND recipient_session_id = ?::uuid
                          AND acked_at IS NOT NULL
                        """.trimIndent()
                    ).use { ps ->
                        ps.setLong(1, req.messageId)
                        ps.setString(2, req.sessionId)
                        alreadyAcked = ps.executeQuery().next()
                    }
                }
            }
            return when {
                updated > 0 -> HttpStatusCode.OK.description("Message acknowledged")
                alreadyAcked -> HttpStatusCode.OK.description("Message already acknowledged")
                else -> HttpStatusCode.NotFound.description(
                    "Message ${req.messageId} not found in session ${req.sessionId}'s mailbox"
                )
            }
        }
    }

    // ============================================================
    //  count_agent_messages (cheap, for harness "should we drain?" check)
    // ============================================================

    object CountUnread : AbstractPostRoute() {
        override val path: String = "/agent/mailbox/count_unread"

        data class Request(val sessionId: String)

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var unread = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT COUNT(*) FROM agent_mailbox_messages " +
                        "WHERE recipient_session_id = ?::uuid AND read_at IS NULL"
                ).use { ps ->
                    ps.setString(1, req.sessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) unread = rs.getInt(1)
                }
            }
            return mapOf("unread" to unread)
        }
    }

    // ============================================================
    //  get_agent_message (single-row fetch for in_reply_to threading)
    // ============================================================

    object GetMessage : AbstractPostRoute() {
        override val path: String = "/agent/mailbox/get"

        data class Request(
            val sessionId: String,
            val messageId: Long,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var row: Map<String, Any?>? = null
            fa.session.useDb { conn ->
                // Either sender or recipient of the session may read; this
                // matches the access policy used by `read_history_tool_call`
                // and lets a sender see whether their message was acked.
                conn.prepareStatement(
                    """
                    SELECT message_id, sender_session_id, recipient_session_id, kind, subject, body,
                           related_artifact_id, in_reply_to_message_id, priority,
                           read_at, acked_at, created_at
                    FROM agent_mailbox_messages
                    WHERE message_id = ?
                      AND (sender_session_id = ?::uuid OR recipient_session_id = ?::uuid)
                    """.trimIndent()
                ).use { ps ->
                    ps.setLong(1, req.messageId)
                    ps.setString(2, req.sessionId)
                    ps.setString(3, req.sessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) row = rs.toMap()
                }
            }
            return row ?: HttpStatusCode.NotFound.description("Message not found or not visible to ${req.sessionId}")
        }
    }

    /** Random UUID helper for tests that want to mock this layer. */
    @Suppress("unused")
    internal fun randomUuid(): String = UUID.randomUUID().toString()
}
