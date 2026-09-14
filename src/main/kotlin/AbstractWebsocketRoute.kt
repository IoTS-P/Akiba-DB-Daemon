package org.iotsplab.akiba.dbDaemon

import io.ktor.http.HttpStatusCode
import io.ktor.server.request.ContentTransformationException
import io.ktor.server.response.respond
import io.ktor.server.routing.Routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger

abstract class AbstractWebsocketRoute {
    /**
     * A request-level failure to report to the caller.
     *
     * [closeCode] lets a route say *how* the request failed instead of always
     * looking like an internal error: the client maps the code back to an HTTP
     * status, so an expected condition (for example "a backup is already
     * running") reaches the UI as a 409 with a readable message rather than a
     * bare 500.
     */
    class AkibaWebsocketException(
        override val message: String,
        val closeCode: Short = CloseReason.Codes.PROTOCOL_ERROR.code,
    ): Exception(message)

    companion object {
        /**
         * Application close code (RFC 6455 reserves 4000-4999) used for
         * "the request conflicts with the current state, retrying later works".
         * The framework maps it to HTTP 409.
         */
        const val WS_CLOSE_CONFLICT: Short = 4409

        /**
         * Character budget for a close reason.
         *
         * RFC 6455 limits the reason to 123 **bytes** (the code takes 2, the frame
         * length another); instance names alone can reach 64 characters, so the
         * message is truncated rather than risking an invalid close frame that
         * would hide the code from the client.
         */
        private const val MAX_CLOSE_REASON_CHARS = 100
    }

    // HTTP path, like 'auth/login'
    abstract val path: String

    // Whether to disable the route or not
    var disabled: Boolean = false

    // Core handler
    @Throws(AkibaWebsocketException::class)
    protected open suspend fun handle(session: DefaultWebSocketServerSession, frames: ReceiveChannel<Frame>) {}

    open fun registerWebsocket(routing: Routing) {
        routing.webSocket(path) {
            if (disabled) {
                call.respond(HttpStatusCode.Locked)
                close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "Not available"))
                return@webSocket
            }

            try {
                globalLogger.trace("Got websocket request $path")
                handle(this, incoming)
            } catch(_: ContentTransformationException) {
                close(CloseReason(CloseReason.Codes.PROTOCOL_ERROR, "Content error"))
            } catch(e: AkibaWebsocketException) {
                // RFC 6455 caps the close reason at 123 bytes; a longer one makes
                // the close frame invalid, the client then never sees the code and
                // reports an opaque failure instead of the real reason.
                close(CloseReason(e.closeCode, e.message.take(MAX_CLOSE_REASON_CHARS)))
            } catch(e: Throwable) {
                close(CloseReason(CloseReason.Codes.INTERNAL_ERROR, "Internal server error"))
                globalLogger.error("Internal server error occurred on websocket $path: ${e.message}")
                globalLogger.error(e.stackTraceToString())
            }

            close(CloseReason(CloseReason.Codes.NORMAL, "Closed normally"))
        }
    }
}