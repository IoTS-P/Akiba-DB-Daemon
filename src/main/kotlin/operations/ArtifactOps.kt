package org.iotsplab.akiba.dbDaemon.operations

import io.ktor.http.HttpStatusCode
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.RouteContext
import org.postgresql.util.PGobject
import java.sql.ResultSet

/**
 * Artifact routes — named, versioned, content-addressed blobs owned by
 * one session and optionally readable by other sessions on the same
 * binary.
 *
 * Access model:
 *  - an artifact is owned by exactly one session;
 *  - `is_public=true` means any session on the same `binary_id` can read
 *    it via `read_agent_artifact`;
 *  - the owner can always read / overwrite its own artifacts; non-owners
 *    cannot update or delete someone else's;
 *  - uniqueness key is `(owner_session_id, name, version)` so previous
 *    versions can be kept for audit / rollback.
 *
 * Beyond owner-vs-not, `InteractionPolicy.canPublishArtifacts` is
 * enforced in the framework layer; the SQL routes are the second line
 * of defense.
 */
object ArtifactOps {

    // ============================================================
    //  Helpers
    // ============================================================

    private fun setJsonb(ps: java.sql.PreparedStatement, index: Int, json: String?) {
        if (json == null) {
            ps.setNull(index, java.sql.Types.OTHER)
        } else {
            val pg = PGobject()
            pg.type = "jsonb"
            pg.value = json
            ps.setObject(index, pg)
        }
    }

    private fun ResultSet.getJsonb(columnLabel: String): String? =
        getObject(columnLabel)?.let { if (it is PGobject) it.value else it.toString() }

    private fun ResultSet.toMap(): Map<String, Any?> = mapOf(
        "artifactId" to getLong("artifact_id"),
        "ownerSessionId" to getString("owner_session_id"),
        "name" to getString("name"),
        "version" to getInt("version"),
        "kind" to getString("kind"),
        "content" to getString("content"),
        "summary" to getString("summary"),
        "metadata" to getJsonb("metadata"),
        "isPublic" to getBoolean("is_public"),
        "supersededBy" to getObject("superseded_by"),
        "createdAt" to getString("created_at"),
    )

    // ============================================================
    //  publish_agent_artifact
    //
    //  Upsert semantics: a row with the same (owner, name, version)
    //  has its content, summary, metadata and is_public replaced.
    //  Callers wanting a new version should query the current version
    //  and pass the next one explicitly.
    // ============================================================

    object PublishArtifact : AbstractPostRoute() {
        override val path: String = "/agent/artifact/publish"

        data class Request(
            val ownerSessionId: String,
            val name: String,
            val kind: String = "data",
            val content: String,
            val summary: String? = null,
            val metadata: String? = null,            // JSON string
            val isPublic: Boolean = false,
            val version: Int = 1,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val validKinds = setOf("data", "finding", "plan", "note", "code", "reference")
            if (req.kind !in validKinds)
                return HttpStatusCode.BadRequest.description(
                    "Invalid kind: ${req.kind}. Must be one of $validKinds"
                )
            if (req.name.isBlank())
                return HttpStatusCode.BadRequest.description("name must not be blank")
            if (req.version <= 0)
                return HttpStatusCode.BadRequest.description("version must be > 0")
            if (req.content.isBlank())
                return HttpStatusCode.BadRequest.description("content must not be blank")

            // Cheap guard; framework usually knows this but defense-in-depth.
            var ownerExists = false
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "SELECT 1 FROM agent_sessions WHERE session_id = ?::uuid"
                ).use { ps ->
                    ps.setString(1, req.ownerSessionId)
                    val rs = ps.executeQuery()
                    if (rs.next()) ownerExists = true
                }
            }
            if (!ownerExists)
                return HttpStatusCode.NotFound.description(
                    "Owner session '${req.ownerSessionId}' not found"
                )

            var artifactId: Long? = null
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO agent_artifacts
                        (owner_session_id, name, version, kind, content, summary, metadata, is_public)
                    VALUES (?::uuid, ?, ?, ?, ?, ?, ?::jsonb, ?)
                    ON CONFLICT (owner_session_id, name, version) DO UPDATE SET
                        kind = EXCLUDED.kind,
                        content = EXCLUDED.content,
                        summary = EXCLUDED.summary,
                        metadata = EXCLUDED.metadata,
                        is_public = EXCLUDED.is_public
                    RETURNING artifact_id
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.ownerSessionId)
                    ps.setString(2, req.name)
                    ps.setInt(3, req.version)
                    ps.setString(4, req.kind)
                    ps.setString(5, req.content)
                    ps.setString(6, req.summary)
                    setJsonb(ps, 7, req.metadata)
                    ps.setBoolean(8, req.isPublic)
                    val rs = ps.executeQuery()
                    if (rs.next()) artifactId = rs.getLong("artifact_id")
                }
            }
            return mapOf(
                "artifactId" to artifactId,
                "ownerSessionId" to req.ownerSessionId,
                "name" to req.name,
                "version" to req.version,
            )
        }
    }

    // ============================================================
    //  read_agent_artifact (by id)
    // ============================================================

    object GetArtifact : AbstractPostRoute() {
        override val path: String = "/agent/artifact/get"

        data class Request(
            val sessionId: String,                  // caller; used to enforce ownership / public access
            val artifactId: Long? = null,
            val ownerSessionId: String? = null,
            val name: String? = null,
            val version: Int? = null,                // null = latest
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            if (req.artifactId == null && (req.ownerSessionId == null || req.name == null))
                return HttpStatusCode.BadRequest.description(
                    "Either artifactId or (ownerSessionId, name[, version]) is required"
                )

            var row: Map<String, Any?>? = null
            val artifactId = req.artifactId
            val ownerSessionId = req.ownerSessionId
            val name = req.name
            val version = req.version
            fa.session.useDb { conn ->
                val (sql, hasId) = when {
                    artifactId != null -> """
                        SELECT artifact_id, owner_session_id, name, version, kind, content, summary,
                               metadata, is_public, superseded_by, created_at
                        FROM agent_artifacts WHERE artifact_id = ?
                    """.trimIndent() to true
                    version != null -> """
                        SELECT artifact_id, owner_session_id, name, version, kind, content, summary,
                               metadata, is_public, superseded_by, created_at
                        FROM agent_artifacts WHERE owner_session_id = ?::uuid AND name = ? AND version = ?
                    """.trimIndent() to false
                    else -> """
                        SELECT artifact_id, owner_session_id, name, version, kind, content, summary,
                               metadata, is_public, superseded_by, created_at
                        FROM agent_artifacts
                        WHERE owner_session_id = ?::uuid AND name = ?
                        ORDER BY version DESC LIMIT 1
                    """.trimIndent() to false
                }
                conn.prepareStatement(sql).use { ps ->
                    if (hasId) {
                        ps.setLong(1, artifactId!!)
                    } else {
                        ps.setString(1, ownerSessionId)
                        ps.setString(2, name)
                        if (version != null) ps.setInt(3, version)
                    }
                    val rs = ps.executeQuery()
                    if (rs.next()) row = rs.toMap()
                }
            }
            if (row == null)
                return HttpStatusCode.NotFound.description("Artifact not found")

            // Access policy: caller must be the owner OR the artifact
            // must be marked is_public. We resolve "same binary" by
            // fetching the caller's binary_id and the owner's binary_id
            // and requiring equality.
            val ownerSid = row["ownerSessionId"] as String
            val isPublic = row["isPublic"] as Boolean
            if (ownerSid != req.sessionId) {
                if (!isPublic)
                    return HttpStatusCode.Forbidden.description(
                        "Artifact is not public; only the owner can read it"
                    )
                val sameBinary = sameBinary(req.sessionId, ownerSid, fa)
                if (!sameBinary)
                    return HttpStatusCode.Forbidden.description(
                        "Artifact is not visible across binaries"
                    )
            }
            return row
        }
    }

    // ============================================================
    //  list_agent_artifacts
    // ============================================================

    object ListArtifacts : AbstractPostRoute() {
        override val path: String = "/agent/artifact/list"

        data class Request(
            val sessionId: String,
            val ownerSessionId: String? = null,      // null = caller's own
            val name: String? = null,                // optional filter
            val includePublic: Boolean = true,
            val limit: Int = 50,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            val effectiveOwner = req.ownerSessionId ?: req.sessionId
            val safeLimit = req.limit.coerceIn(1, 500)
            val results: MutableList<Map<String, Any?>> = mutableListOf()

            fa.session.useDb { conn ->
                val conditions = mutableListOf("owner_session_id = ?::uuid")
                if (req.includePublic) conditions.add("is_public = true")
                if (!req.name.isNullOrBlank()) conditions.add("name = ?")
                val sql = """
                    SELECT DISTINCT ON (name)
                           artifact_id, owner_session_id, name, version, kind, content, summary,
                           metadata, is_public, superseded_by, created_at
                    FROM agent_artifacts
                    WHERE ${conditions.joinToString(" OR ")}
                    ORDER BY name, version DESC
                    LIMIT ?
                """.trimIndent()
                conn.prepareStatement(sql).use { ps ->
                    var idx = 1
                    ps.setString(idx++, effectiveOwner)
                    if (req.includePublic) ps.setBoolean(idx++, true)
                    if (!req.name.isNullOrBlank()) ps.setString(idx++, req.name)
                    ps.setInt(idx, safeLimit)
                    val rs = ps.executeQuery()
                    while (rs.next()) results.add(rs.toMap())
                }
            }
            return results
        }
    }

    // ============================================================
    //  delete_agent_artifact (owner-only)
    // ============================================================

    object DeleteArtifact : AbstractPostRoute() {
        override val path: String = "/agent/artifact/delete"

        data class Request(
            val sessionId: String,                   // caller; must be the owner
            val artifactId: Long,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val req = ctx.receive<Request>()
            val fa = try { fastAccess(ctx) } catch (e: FastAccessException) { return e.code }

            var deleted = 0
            fa.session.useDb { conn ->
                conn.prepareStatement(
                    "DELETE FROM agent_artifacts WHERE artifact_id = ? AND owner_session_id = ?::uuid"
                ).use { ps ->
                    ps.setLong(1, req.artifactId)
                    ps.setString(2, req.sessionId)
                    deleted = ps.executeUpdate()
                }
            }
            return if (deleted > 0) HttpStatusCode.OK.description("Artifact deleted")
            else HttpStatusCode.NotFound.description(
                "Artifact ${req.artifactId} not found or not owned by ${req.sessionId}"
            )
        }
    }

    // ============================================================
    //  Internal: cross-binary visibility check
    // ============================================================

    /** Public artifacts are shared within a single binary's workspace only. */
    private fun sameBinary(
        callerSessionId: String,
        ownerSessionId: String,
        fa: AbstractPostRoute.FastAccessStruct,
    ): Boolean {
        if (callerSessionId == ownerSessionId) return true
        var callerBin: Int? = null
        var ownerBin: Int? = null
        fa.session.useDb { conn ->
            conn.prepareStatement(
                "SELECT binary_id FROM agent_sessions WHERE session_id = ?::uuid"
            ).use { ps ->
                ps.setString(1, callerSessionId)
                val rs = ps.executeQuery()
                if (rs.next()) {
                    val raw = rs.getObject("binary_id")
                    if (raw != null) callerBin = (raw as Number).toInt()
                }
            }
            conn.prepareStatement(
                "SELECT binary_id FROM agent_sessions WHERE session_id = ?::uuid"
            ).use { ps ->
                ps.setString(1, ownerSessionId)
                val rs = ps.executeQuery()
                if (rs.next()) {
                    val raw = rs.getObject("binary_id")
                    if (raw != null) ownerBin = (raw as Number).toInt()
                }
            }
        }
        return callerBin != null && callerBin == ownerBin
    }
}
