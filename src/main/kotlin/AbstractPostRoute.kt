package org.iotsplab.akiba.dbDaemon

import io.ktor.server.routing.*
import io.ktor.http.*
import io.ktor.server.request.ContentTransformationException
import io.ktor.server.request.header
import io.ktor.server.response.respond
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.FrameType
import io.ktor.websocket.close
import kotlinx.coroutines.channels.consume
import kotlinx.coroutines.channels.consumeEach
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger
import org.iotsplab.akiba.dbDaemon.dbutil.UserDatabaseSession
import org.iotsplab.akiba.dbDaemon.operations.PGInstances
import org.iotsplab.akiba.dbDaemon.operations.PGInstances.tokens
import java.util.UUID

abstract class AbstractPostRoute {

    data class FastAccessStruct (
        val token: UUID,
        val user: String,
        val session: UserDatabaseSession
    )

    class FastAccessException(val code: HttpStatusCode): Exception()

    // HTTP path, like 'auth/login'
    abstract val path: String

    // Whether to disable the route or not
    var disabled: Boolean = false

    // Whether to respond or not
    var respond: Boolean = true

    // Core handler
    protected abstract suspend fun handle(ctx: RouteContext): Any?

    /** The database name when [e] comes from a database that does not exist. */
    private fun missingDatabase(e: Throwable): String? =
        generateSequence(e) { it.cause }
            .filterIsInstance<java.sql.SQLException>()
            .mapNotNull { it.message }
            .firstOrNull { it.contains("database") && it.contains("does not exist") }

    open fun registerPost(routing: Routing) {
        routing.post(path) {
            if (disabled) {
                call.respond(HttpStatusCode.Locked)
                return@post
            }

            val ctx = RouteContext(call)

            val result = try {
                globalLogger.trace("Got POST request $path")
                handle(ctx).let {
                    globalLogger.trace("POST Response: {}", it)
                    it
                }
            } catch(e: ContentTransformationException) {
                // User data not valid
                globalLogger.error("Content format error: ${e.message}")
                call.respond(HttpStatusCode.BadRequest)
                return@post
            } catch(e: Exception) {
                // A database that no longer exists is the caller's problem, and the caller
                // can fix it: say which one is gone instead of a bare 500.
                missingDatabase(e)?.let { message ->
                    call.respond(HttpStatusCode.NotFound, mapOf("error" to message))
                    globalLogger.error("Database missing on POST $path: $message")
                    return@post
                }
                call.respond(HttpStatusCode.InternalServerError)
                globalLogger.error("Internal server error occurred on POST $path: ${e.message}")
                globalLogger.error(e.stackTraceToString())
                return@post
            }

            if (respond) {
                when (result) {
                    null -> call.respond(HttpStatusCode.OK)
                    is HttpStatusCode -> call.respond(result)
                    else -> call.respond(result)
                }
            }
        }
    }

    companion object {
        @Throws(FastAccessException::class)
        fun fastAccess(ctx: RouteContext): FastAccessStruct {
            val token = try {
                ctx.call.request.header(HttpHeaders.Authorization) ?.let { UUID.fromString(it) }
                    ?: throw FastAccessException(
                        HttpStatusCode.Unauthorized.description("No authorization token provided"))
            } catch (_: Exception) {
                throw FastAccessException(
                    HttpStatusCode.BadRequest.description("Invalid authorization token"))
            }
            val user = tokens[token]
                ?: throw FastAccessException(
                    HttpStatusCode.BadRequest.description("Owner of the token not found"))

            val dbSession: UserDatabaseSession = try {
                PGInstances.tokenSessions
                    .getResource(token, user)
                    ?: throw FastAccessException(
                        HttpStatusCode.BadRequest.description("No database session found"))
            } catch (e: Exception) {
                throw FastAccessException(
                    HttpStatusCode.BadRequest.description("Get session failed: ${e.message}"))
            }

            PGInstances.tokenSessions.renew(token, user)
            return FastAccessStruct(token, user, dbSession)
        }
    }
}