package org.iotsplab.akiba.dbDaemon.operations

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.plugins.origin
import io.ktor.server.request.header
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.receiveDeserialized
import io.ktor.server.websocket.sendSerialized
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.AbstractWebsocketRoute
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.config
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger
import org.iotsplab.akiba.dbDaemon.RouteContext
import org.iotsplab.akiba.dbDaemon.dbutil.UserDatabaseSession
import org.iotsplab.akiba.dbDaemon.dbutil.UserStore
import org.iotsplab.akiba.dbDaemon.operations.PGInstances.CreateInstance.createLock
import org.iotsplab.akiba.dbDaemon.token.ResourceData
import org.iotsplab.akiba.dbDaemon.token.TimedResourceMap
import org.iotsplab.akiba.dbDaemon.token.TimedResourceMap.NotLockedException
import org.iotsplab.akiba.dbDaemon.token.TimedResourceMap.NotOwnedException
import java.io.File
import java.nio.file.Path
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.io.path.absolute
import kotlin.io.path.exists
import kotlin.io.path.notExists
import kotlin.concurrent.withLock
import kotlin.jvm.Throws

object PGInstances {

    data class InstanceMetadata (
        val port: Int,
        val owner: String,   // Also the name of the only database contained in this instance
        var logicalPrior: String?    // Last backup restored, used to build backup trees
    )

    const val MIN_PORT = 31800
    const val MAX_PORT = 31999

    // Major version of postgresql
    var pgVer: Int = -1
    val instanceRoot: Path = Path.of(config.instanceRoot).absolute()

    // instances created
    // key: instance name
    // value: necessary information of an instance
    lateinit var instances: MutableMap<String, InstanceMetadata>

    fun initialize() {
        val mapFile = File(config.instanceMapFile)
        instances = if (mapFile.exists()) {
            jacksonObjectMapper().readValue(mapFile)
        } else
            mutableMapOf()

        val getVer = ProcessBuilder("pg_config", "--version").start()
        getVer.waitFor()
        pgVer = getVer.inputStream.bufferedReader().readText()
            .substringAfter("PostgreSQL")
            .substringBefore(".")
            .trim()
            .toInt()
    }

    /**
     * User session map: Map storing tokens and relative resources.
     *
     * - Generic `K` (`UUID`): Token.
     * - Generic `V` (`UserSession`): Session of this token, containing the instance opened by this token.
     * - Generic `R` (`String`): Owner username
     */
    class UserSessionMap: TimedResourceMap<UUID, UserDatabaseSession, String>(
        Duration.ofSeconds(config.maxLockWaitingTime.toLong())
    ) {
        @Throws(NotLockedException::class, NotOwnedException::class)
        override fun unlock(resourceKey: UUID, owner: String) {
            operationLock.withLock {
                val ownerInfo: Pair<ResourceData<String>, UserDatabaseSession?>
                    = resourcesOwned[resourceKey] ?: throw NotLockedException()

                if (ownerInfo.first.owner != owner) throw NotOwnedException()
                else {
                    val removed = resourcesOwned.remove(resourceKey)
                    removed?.first?.timer?.get()?.cancel(false)
                    // Release all sessions owned and close the DB session
                    removed?.second?.let { session ->
                        session.tableSessions.unlockAllOfOwner(owner)
                        session.close()
                    }
                }
            }
        }

        override fun renew(resourceKey: UUID, owner: String) {
            operationLock.withLock {
                super.renew(resourceKey, owner)
                val ownerInfo = resourcesOwned[resourceKey] ?: throw NotLockedException()
                if (ownerInfo.first.owner != owner) throw NotOwnedException()
                else
                    ownerInfo.second?.tableSessions?.renewAll()
            }
        }

        override fun releaseHook(key: UUID, resource: Pair<ResourceData<String>, UserDatabaseSession?>, owner: String) {
            resource.second?.let { session ->
                session.tableSessions.unlockAllOfOwner(owner)
                session.close()
            }
        }

        fun unlockAllOfOwner(owner: String) {
            val keysToUnlock = operationLock.withLock {
                resourcesOwned.keys.toList().filter {
                    resourcesOwned[it]?.first?.owner == owner
                }
            }
            keysToUnlock.forEach { key ->
                unlock(key, owner)
            }
        }

        fun instanceIsInUse(instanceName: String): Boolean {
            return resourcesOwned.values.any { it.second?.instanceName == instanceName }
        }
    }

    val tokens: ConcurrentHashMap<UUID, String> = ConcurrentHashMap()
    val tokenSessions: UserSessionMap = UserSessionMap()

    /**
     * Get a connection of a token to a table
     *
     * @param tableName Table name
     * @param token Client token
     * @return Connection and error message, there will be one and only one that is not null.
     *         There could be many error messages, including bad tableName (table name not found),
     *         bad token (token not found), unlocked table (table not locked),
     *         conflict lock (table locked by any other client)
     */
    fun getConnection(tableName: String, token: UUID): Pair<Connection?, HttpStatusCode?> {
        val user = tokens[token]
            ?: return null to HttpStatusCode.BadRequest.description("Token $token not found")
        try {
            val tokenSession = tokenSessions.getResource(
                token, user)
                ?: return null to HttpStatusCode.BadRequest.description("Token $token invalid")
            val tableSession = tokenSession.tableSessions.getResource(tableName, user)
                ?: return null to HttpStatusCode.BadRequest.description("Table $tableName invalid or unlocked")
            return tableSession to null
        } catch (_: NotLockedException) {
            return null to HttpStatusCode.Conflict.description("Table $tableName is not locked")
        } catch (_: NotOwnedException) {
            return null to HttpStatusCode.Conflict.description("Table $tableName is locked by other clients")
        }
    }

    @Throws(PGPortFullException::class, IllegalStateException::class)
    private fun createPsqlInstance(userName: String, instanceName: String): InstanceMetadata {
        val port = selectInstancePort()
        val scriptPath = File("resources/initialize_pg_instance.sh")
        if (!scriptPath.exists()) {
            throw IllegalStateException(
                "Initialization script not found at ${scriptPath.absolutePath} " +
                "(working dir = ${File(".").absolutePath}). The akiba_db_daemon distribution " +
                "must be unpacked with its resources/ directory intact."
            )
        }
        val process = ProcessBuilder("bash", scriptPath.path)
        val env = process.environment()
        env["INSTANCE_NAME"] = instanceName
        env["INSTANCE_ROOT"] = instanceRoot.toString()
        env["BACKUP_ROOT"] = config.backupRoot
        env["USER_NAME"] = userName
        env["PORT"] = port.toString()
        val p = process.redirectErrorStream(true).start()
        // Capture once — we want to log the full output AND embed (a truncated form of) it
        // in the IllegalStateException. Reading the stream twice would block.
        val output = p.inputStream.bufferedReader().readText()
        p.waitFor()
        if (p.exitValue() != 0) {
            globalLogger.error(
                "Instance $instanceName initialization script (exit=${p.exitValue()}) full output:\n$output")
            throw IllegalStateException(
                "Instance $instanceName initialization failed (exit=${p.exitValue()}); " +
                "see daemon log for full output. Tail: " +
                output.lines().filter { it.isNotBlank() }.takeLast(3).joinToString(" | ")
            )
        }
        return InstanceMetadata(port, userName, null)
    }

    @Throws(IllegalStateException::class, IllegalArgumentException::class)
    private fun deletePsqlInstanceUnchecked(instanceName: String) {
        // We must check the instance name to avoid directory traversal
        // After all running "rm -rf /" is not a good idea :(
        if (instanceName.contains("/"))
            throw IllegalArgumentException("Instance name cannot contain /")
        val instancePath = instanceRoot.resolve(instanceName)
        if (instancePath.notExists())
            return
        val process = ProcessBuilder("sudo", "rm", "-rf", "$instanceRoot/$instanceName")
        val p = process.redirectErrorStream(true).start()
        p.waitFor()
        if (instancePath.exists())
            throw IllegalStateException(
                "Instance $instanceName deletion failed, output:\n" + p.inputStream.bufferedReader().readText())
    }

    @Throws(IllegalArgumentException::class)
    fun instanceIsOn(instanceName: String): Boolean {
        val instanceInfo = instances[instanceName]
            ?: throw IllegalArgumentException("Instance $instanceName not found")
        val process = ProcessBuilder(
            "pg_isready", "-h", "127.0.0.1", "-p", instanceInfo.port.toString(),
            "-U", instanceInfo.owner, "-d", instanceInfo.owner).start()
        process.waitFor()
        return process.exitValue() == 0
    }

    /**
     * Returns true iff the cluster is currently in PostgreSQL recovery / hot-standby mode
     * (i.e. `pg_is_in_recovery()` returns `t`). In that state every write transaction is
     * rejected with `cannot execute ... in a read-only transaction`. Used by
     * `BackupManager.restoreBackup` as a defence-in-depth check after a pgbackrest restore
     * — if the cluster did not auto-promote despite `--target-action=promote`, we fall
     * back to `pg_ctl promote`.
     *
     * Returns false (best-effort) if the instance is not yet accepting connections at all,
     * or if the helper psql call itself errors out for any unrelated reason.
     */
    @Throws(IllegalArgumentException::class)
    fun instanceIsInRecovery(instanceName: String): Boolean {
        val instanceInfo = instances[instanceName]
            ?: throw IllegalArgumentException("Instance $instanceName not found")
        if (!instanceIsOn(instanceName)) return false
        val process = ProcessBuilder(
            "sudo", "-u", "postgres",
            "psql", "-h", "127.0.0.1", "-p", instanceInfo.port.toString(),
            "-U", instanceInfo.owner, "-d", instanceInfo.owner,
            "-tAqc", "SELECT pg_is_in_recovery();"
        ).redirectErrorStream(true).start()
        process.waitFor()
        if (process.exitValue() != 0) return false
        val output = process.inputStream.bufferedReader().readText().trim()
        return output.equals("t", ignoreCase = true) || output.equals("true", ignoreCase = true)
    }

    @Throws(IllegalArgumentException::class, IllegalStateException::class)
    fun startInstance(instanceName: String) {
        val instanceInfo = instances[instanceName]
            ?: throw IllegalArgumentException("Instance $instanceName not found")
        if (instanceIsOn(instanceName))
            return

        val startProcess = ProcessBuilder("sudo", "-i", "-u", "postgres",
            "/usr/lib/postgresql/$pgVer/bin/pg_ctl", "-D", "$instanceRoot/$instanceName", "-w", "start").start()
        startProcess.waitFor()

        if (!instanceIsOn(instanceName))
            throw IllegalStateException("Instance $instanceName failed to start, process output: " +
                startProcess.inputStream.bufferedReader().readText()
            )
    }

    @Throws(IllegalArgumentException::class, IllegalStateException::class)
    fun shutdownInstance(instanceName: String) {
        val instanceInfo = instances[instanceName]
            ?: throw IllegalArgumentException("Instance $instanceName not found")
        if (!instanceIsOn(instanceName))
            return

        val shutdownProcess = ProcessBuilder("sudo", "-i", "-u", "postgres",
            "/usr/lib/postgresql/$pgVer/bin/pg_ctl", "-D", "$instanceRoot/$instanceName", "-w", "stop").start()
        shutdownProcess.waitFor()

        if (instanceIsOn(instanceName))
            throw IllegalStateException("Instance $instanceName failed to stop, process output: " +
                shutdownProcess.inputStream.bufferedReader().readText()
            )
    }

    @Throws(PGPortFullException::class)
    private fun selectInstancePort(): Int {
        for (port in MIN_PORT..MAX_PORT) {
            if (instances.values.any { it.port == port })
                continue
            return port
        }
        throw PGPortFullException()
    }

    @Throws(IllegalArgumentException::class, IllegalStateException::class)
    fun shutdownAllInstances() {
        instances.forEach { (name, metadata) ->
            if (instanceIsOn(name))
                shutdownInstance(name)
        }
    }

    class PGPortFullException: Exception("PostgreSQL instance full, no local port available")

    // ============================================================
    // -------------------------- ROUTES --------------------------
    // ============================================================

    object ClientLogin: AbstractPostRoute() {
        override val path: String = "/instance/login"

        data class AuthData (
            val username: String,
            val password: String = ""
        )

        /**
         * Verify the supplied credentials against [UserStore] and, on success,
         * issue the session token that every other route authenticates with.
         *
         * Two outcomes deserve a note:
         *
         *  - **Bootstrap.** When the credential store holds no users at all
         *    (fresh deployment, or a pre-existing deployment that upgraded to a
         *    version with authentication), the first login *registers* the
         *    supplied username + password and marks it as the administrator.
         *    Without this, existing deployments would be locked out of their own
         *    instance data, which is owned by that username.
         *  - **Failure modes are deliberately indistinguishable.** An unknown
         *    username and a wrong password both return 401 with the same
         *    message, so the daemon does not confirm which usernames exist.
         *
         * Brute-force protection is the third: a rejected login is answered only
         * after [UserStore.LOGIN_FAILURE_DELAY], and
         * [UserStore.MAX_LOGIN_FAILED_ATTEMPTS] failures inside a day lock the
         * account for a day. That policy is skipped for callers on the loopback
         * interface — the co-located Akiba server, which relays credentials a
         * human already entered at the web login and is not an attacker. Without
         * the exemption, anyone able to reach this port could lock an account
         * here and thereby cut the deployment off from its own daemon.
         */
        override suspend fun handle(ctx: RouteContext): Any? {
            val authData = ctx.receive<AuthData>()
            val username = authData.username.trim()
            if (username.isEmpty())
                return HttpStatusCode.BadRequest.description("Username must not be empty")

            val peer = ctx.call.request.origin.remoteHost
            val trustedPeer = peer == "127.0.0.1" || peer == "::1" || peer == "localhost"

            return when (val verdict = UserStore.login(username, authData.password, enforceLock = !trustedPeer)) {
                UserStore.LoginResult.BadCredentials -> {
                    // Hold the answer back so the endpoint is not a fast oracle for
                    // guessing passwords (mirrors the framework's web login).
                    delay(UserStore.LOGIN_FAILURE_DELAY.toMillis())
                    HttpStatusCode.Unauthorized.description("Invalid username or password")
                }

                is UserStore.LoginResult.Locked -> {
                    delay(UserStore.LOGIN_FAILURE_DELAY.toMillis())
                    globalLogger.warn("Rejected login for locked daemon account '$username'")
                    HttpStatusCode.Locked.description(
                        "Account locked after too many failed sign-ins. It unlocks at " +
                            "${verdict.until}, or an administrator can unlock it."
                    )
                }

                is UserStore.LoginResult.StoreUnavailable ->
                    HttpStatusCode.InternalServerError.description(
                        "Credential store unavailable: ${verdict.reason}")

                UserStore.LoginResult.Success, UserStore.LoginResult.Bootstrapped -> {
                    val token = UUID.randomUUID()
                    tokens[token] = username
                    val bootstrapped = verdict === UserStore.LoginResult.Bootstrapped
                    if (bootstrapped) {
                        globalLogger.warn(
                            "Credential store was empty: '$username' has been registered as the " +
                                "first administrator of this daemon. Later logins must match this " +
                                "password.")
                    }
                    mapOf(
                        "token" to token,
                        "username" to username,
                        "isAdmin" to UserStore.isAdmin(username),
                        "bootstrapped" to bootstrapped,
                    )
                }
            }
        }
    }

    /**
     * Register an additional credential.
     *
     * Creating further users requires an administrator token (the first user
     * never does — see [UserStore.register]). The framework's HTTP server calls
     * this when an administrator creates a web account, so that the web login
     * and the daemon credential stay in sync.
     */
    object ClientRegister: AbstractPostRoute() {
        override val path: String = "/instance/register"

        data class RegisterData (
            val username: String,
            val password: String = "",
            val isAdmin: Boolean = false,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val data = ctx.receive<RegisterData>()
            val requester = ctx.call.request.header(HttpHeaders.Authorization)
                ?.let { raw -> runCatching { UUID.fromString(raw) }.getOrNull() }
                ?.let { tokens[it] }
            val authorized = requester != null && UserStore.isAdmin(requester)

            return when (UserStore.register(
                data.username, data.password, data.isAdmin, authorized
            )) {
                UserStore.RegisterResult.Created ->
                    mapOf(
                        "username" to data.username.trim(),
                        "isAdmin" to UserStore.isAdmin(data.username.trim()),
                    )
                UserStore.RegisterResult.AlreadyExists ->
                    HttpStatusCode.Conflict.description(
                        "User '${data.username.trim()}' already exists")
                UserStore.RegisterResult.InvalidName ->
                    HttpStatusCode.BadRequest.description(
                        "Invalid username: expected ${UserStore.USERNAME_PATTERN}")
                UserStore.RegisterResult.WeakPassword ->
                    HttpStatusCode.BadRequest.description(
                        "Password must be at least ${UserStore.MIN_PASSWORD_LENGTH} characters")
                UserStore.RegisterResult.NotAuthorized ->
                    HttpStatusCode.Unauthorized.description(
                        "Only an administrator may create new users")
                UserStore.RegisterResult.StoreUnavailable ->
                    HttpStatusCode.InternalServerError.description(
                        "Credential store unavailable")
            }
        }
    }

    /**
     * List the instances owned by the caller.
     *
     * The daemon historically had no way to enumerate instances, which forced
     * the frontend to remember names in `localStorage` and probe them one by
     * one. Now that instances are namespaced by the authenticated user, the
     * daemon can answer the question the UI actually asks: "which instances
     * does *this* user have?".
     */
    object ListInstances: AbstractPostRoute() {
        override val path: String = "/instance/list"

        override suspend fun handle(ctx: RouteContext): Any? {
            val token = ctx.call.request.header(HttpHeaders.Authorization)
                ?.let { raw -> runCatching { UUID.fromString(raw) }.getOrNull() }
                ?: return HttpStatusCode.Unauthorized.description("No authorization token provided")
            val user = tokens[token]
                ?: return HttpStatusCode.BadRequest.description("Token $token not found")

            val owned = instances.entries
                .filter { it.value.owner == user }
                .map { (name, meta) ->
                    mapOf(
                        "name" to name,
                        "port" to meta.port,
                        "logicalPrior" to meta.logicalPrior,
                        "running" to instanceIsOn(name),
                    )
                }

            return mapOf(
                "username" to user,
                "isAdmin" to UserStore.isAdmin(user),
                "instances" to owned,
            )
        }
    }

    /**
     * Mint a derived credential for the caller's own identity.
     *
     * The framework's HTTP server spawns child JVMs (workflow runs, manual agent
     * workers) that talk to the daemon on their own. Those children must present
     * the *owner's* identity, because `/instance/connect` checks ownership — so
     * the parent asks for a derived secret here instead of copying the account
     * password into the generated config file.
     */
    object WorkerGrant: AbstractPostRoute() {
        override val path: String = "/instance/worker-grant"

        /** Default lifetime of a derived credential: 12 hours. */
        const val DEFAULT_TTL_SECONDS: Long = 12 * 3600
        const val MAX_TTL_SECONDS: Long = 30L * 24 * 3600

        data class GrantRequest(
            val label: String = "worker",
            val ttlSeconds: Long = DEFAULT_TTL_SECONDS,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val token = ctx.call.request.header(HttpHeaders.Authorization)
                ?.let { raw -> runCatching { UUID.fromString(raw) }.getOrNull() }
                ?: return HttpStatusCode.Unauthorized.description("No authorization token provided")
            val user = tokens[token]
                ?: return HttpStatusCode.BadRequest.description("Token $token not found")

            val req = ctx.receive<GrantRequest>()
            val ttl = req.ttlSeconds.coerceIn(60L, MAX_TTL_SECONDS)
            val secret = UserStore.grantDerived(user, req.label, ttl)
                ?: return HttpStatusCode.InternalServerError.description(
                    "No credential record for '$user'")

            return mapOf(
                "username" to user,
                "secret" to secret,
                "expiresInSeconds" to ttl,
            )
        }
    }

    /**
     * Change another user's password.
     *
     * The framework mirrors every account into this credential store, so a
     * password change on the web side has to be applied here too — otherwise the
     * account can sign in but every instance operation is rejected. Only an
     * administrator token may call this.
     */
    object SetPassword: AbstractPostRoute() {
        override val path: String = "/instance/set-password"

        data class SetPasswordData (
            val username: String,
            val password: String = "",
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val requester = ctx.call.request.header(HttpHeaders.Authorization)
                ?.let { raw -> runCatching { UUID.fromString(raw) }.getOrNull() }
                ?.let { tokens[it] }
                ?: return HttpStatusCode.Unauthorized.description(
                    "No authorization token provided")

            if (!UserStore.isAdmin(requester))
                return HttpStatusCode.Unauthorized.description(
                    "Only an administrator may change another user's password")

            val data = ctx.receive<SetPasswordData>()
            if (data.password.length < UserStore.MIN_PASSWORD_LENGTH)
                return HttpStatusCode.BadRequest.description(
                    "Password must be at least ${UserStore.MIN_PASSWORD_LENGTH} characters")

            val name = data.username.trim()
            if (!UserStore.hasUser(name))
                return HttpStatusCode.NotFound.description("User '$name' not found")

            globalLogger.warn("Administrator '$requester' changed the password of '$name'")
            return if (UserStore.setPassword(name, data.password))
                mapOf("username" to name, "message" to "Password updated")
            else
                HttpStatusCode.InternalServerError.description(
                    "Failed to update the credential of '$name'")
        }
    }

    /**
     * Lift a brute-force lock.
     *
     * The daemon enforces the same policy as the framework's web login (three
     * seconds before a rejection, five failures inside a day lock the account for
     * a day), so a locked account has to be unlockable here too — the framework
     * calls this when an administrator uses `/admin/users/{id}/unlock`. Only an
     * administrator token may call it.
     */
    object Unlock: AbstractPostRoute() {
        override val path: String = "/instance/unlock"

        data class UnlockData (
            val username: String,
        )

        override suspend fun handle(ctx: RouteContext): Any? {
            val requester = ctx.call.request.header(HttpHeaders.Authorization)
                ?.let { raw -> runCatching { UUID.fromString(raw) }.getOrNull() }
                ?.let { tokens[it] }
                ?: return HttpStatusCode.Unauthorized.description(
                    "No authorization token provided")

            if (!UserStore.isAdmin(requester))
                return HttpStatusCode.Unauthorized.description(
                    "Only an administrator may unlock an account")

            val name = ctx.receive<UnlockData>().username.trim()
            if (name.isEmpty())
                return HttpStatusCode.BadRequest.description("Username must not be empty")
            if (!UserStore.hasUser(name))
                return HttpStatusCode.NotFound.description("User '$name' not found")

            globalLogger.warn("Administrator '$requester' unlocked the account of '$name'")
            return mapOf("username" to name, "message" to "Account unlocked")
                .also { UserStore.unlock(name) }
        }
    }

    object ClientLogout: AbstractPostRoute() {
        override val path: String = "/instance/logout"

        override suspend fun handle(ctx: RouteContext): Any? {
            val token = UUID.fromString(ctx.header("Authorization"))
            val user = tokens[token]
                ?: return HttpStatusCode.BadRequest.description("Token $token not found")
            try {
                tokenSessions.unlock(token, user)
            } catch (_: NotLockedException) {
                // Session may have already timed out, ignore
            } catch (_: NotOwnedException) {
                return HttpStatusCode.BadRequest.description("Token not owned by user")
            }
            return HttpStatusCode.OK.description("Logout successful")
        }
    }

    private var creatingInstances: String? = null

    object CreateInstance: AbstractWebsocketRoute() {
        override val path: String = "/ws/instance/create"
        private val createLock: Mutex = Mutex()

        data class CreateData (
            val token: String,
            val instanceName: String
        )

        override suspend fun handle(session: DefaultWebSocketServerSession, frames: ReceiveChannel<Frame>) {
            val data = session.receiveDeserialized<CreateData>()
            val user = tokens[UUID.fromString(data.token)]
                ?: throw AkibaWebsocketException("Token ${data.token} not found")
            instances[data.instanceName]
                ?.let { throw AkibaWebsocketException("Instance ${data.instanceName} already exists") }

            try {
                createLock.withLock {
                    creatingInstances = data.instanceName
                    withContext(Dispatchers.IO) {
                        instances[data.instanceName] = createPsqlInstance(user, data.instanceName)
                    }
                    creatingInstances = null
                    globalLogger.info("PostgreSQL instance ${data.instanceName} created for user $user")
                }
            } catch (_: PGPortFullException) {
                throw AkibaWebsocketException("No more ports available")
            } catch (e: IllegalStateException) {
                throw AkibaWebsocketException("Instance ${data.instanceName} initialization failed:\n" +
                        "${e.message}")
            }

            session.sendSerialized(mapOf("msg" to "Instance ${data.instanceName} created"))
        }
    }

    /**
     * Connect to an instance
     *
     * If the instance doesn't exist, return error.
     * If the instance isn't started, start it first and then connect
     */
    object ConnectToInstance: AbstractPostRoute() {
        override val path: String = "/instance/connect"

        override suspend fun handle(ctx: RouteContext): Any? {
            val instanceName = ctx.receive<Map<String, String>>()["instanceName"]
                ?: return HttpStatusCode.BadRequest.description("Instance name not provided")
            val token = ctx.call.request.header(HttpHeaders.Authorization) ?.let { UUID.fromString(it) }
                ?: return HttpStatusCode.Unauthorized.description("No authorization token provided")
            val user = tokens[token]
                ?: return HttpStatusCode.BadRequest.description("Token $token not found")
            val metadata = instances[instanceName]
                ?: return HttpStatusCode.BadRequest.description("Instance $instanceName not found")

            // Instances are namespaced by their owner: a client may only open a
            // connection to an instance it owns. Without this check the token was
            // just a liveness token and any user could read (and write) any other
            // user's analysis data by naming the instance.
            if (metadata.owner != user)
                return HttpStatusCode.Forbidden.description(
                    "Instance $instanceName is owned by another user")

            if (!instanceIsOn(instanceName))
                startInstance(instanceName)

            try {
                tokenSessions.lock(token, user, UserDatabaseSession(instanceName))
            } catch (_: TimedResourceMap.AlreadyLockedException) {
                return HttpStatusCode.Conflict.description("Instance $instanceName is already connected")
            } catch (_: NotOwnedException) {
                return HttpStatusCode.Conflict.description("Instance $instanceName is connected by others")
            }

            return HttpStatusCode.OK.description("Connected to instance $instanceName")
        }
    }

    /**
     * Disconnect from an instance
     *
     * Only disconnect, and don't shut down the instance
     */
    object DisconnectFromInstance: AbstractPostRoute() {
        override val path: String = "/instance/disconnect"

        override suspend fun handle(ctx: RouteContext): Any? {
            val instanceName = ctx.receive<Map<String, String>>()["instanceName"]
                ?: return HttpStatusCode.BadRequest.description("Instance name not provided")
            val token = ctx.call.request.header(HttpHeaders.Authorization) ?.let { UUID.fromString(it) }
                ?: return HttpStatusCode.Unauthorized.description("No authorization token provided")
            val user = tokens[token]
                ?: return HttpStatusCode.BadRequest.description("Token $token not found")
            try {
                tokenSessions.unlock(token, user)
            } catch (_: NotLockedException) {
                return HttpStatusCode.Conflict.description("Instance $instanceName is not locked")
            } catch (_: NotOwnedException) {
                return HttpStatusCode.Conflict.description("Instance $instanceName is used by other users")
            }
            return HttpStatusCode.OK.description("Disconnected from instance $instanceName")
        }
    }

    /**
     * Shutdown an instance
     *
     * If this token is using this instance, unlock it first. After that, if this instance is used by other users,
     * return conflict.
     */
    object ShutdownInstance: AbstractPostRoute() {
        override val path: String = "/instance/shutdown"

        override suspend fun handle(ctx: RouteContext): Any? {
            // Need to provide instance name
            val instanceName = ctx.receive<Map<String, String>>()["instanceName"]
                ?: return HttpStatusCode.BadRequest.description("Instance name not provided")
            // Need to provide a token
            val token = ctx.call.request.header(HttpHeaders.Authorization) ?.let { UUID.fromString(it) }
                ?: return HttpStatusCode.Unauthorized.description("No authorization token provided")
            // This token need to correspond to a user
            val user = tokens[token]
                ?: return HttpStatusCode.BadRequest.description("Token $token not found")
            // This instance need to exist
            instances[instanceName]
                ?: return HttpStatusCode.BadRequest.description("Instance $instanceName not found")
            // The owner of this instance needs to be the user
            if (instances[instanceName]!!.owner != user)
                return HttpStatusCode.BadRequest.description("You are not the owner of instance $instanceName")
            tokenSessions.unlock(token, user)
            if (tokenSessions.instanceIsInUse(instanceName))
                return HttpStatusCode.Conflict.description("Instance $instanceName is still in use")

            shutdownInstance(instanceName)
            return mapOf("token" to token)
        }
    }

    object DeleteInstance: AbstractPostRoute() {
        override val path: String = "/instance/delete"

        override suspend fun handle(ctx: RouteContext): Any? {
            // Need to provide instance name
            val instanceName = ctx.receive<Map<String, String>>()["instanceName"]
                ?: return HttpStatusCode.BadRequest.description("Instance name not provided")
            // Need to provide a token
            val token = ctx.call.request.header(HttpHeaders.Authorization) ?.let { UUID.fromString(it) }
                ?: return HttpStatusCode.Unauthorized.description("No authorization token provided")
            // This token need to correspond to a user
            val user = tokens[token]
                ?: return HttpStatusCode.BadRequest.description("Token $token not found")
            // This instance need to exist
            instances[instanceName]
                ?: return HttpStatusCode.BadRequest.description("Instance $instanceName not found")
            // The owner of this instance needs to be the user
            if (instances[instanceName]!!.owner != user)
                return HttpStatusCode.BadRequest.description("You are not the owner of instance $instanceName")
            // This instance need to be stopped or unused
            if (tokenSessions.instanceIsInUse(instanceName))
                return HttpStatusCode.Conflict.description("Instance $instanceName is still in use")
            // This instance need to be shut down before deletion
            if (instanceIsOn(instanceName))
                shutdownInstance(instanceName)

            try {
                deletePsqlInstanceUnchecked(instanceName)
            } catch (e: Exception) {
                return HttpStatusCode.Conflict.description("Instance $instanceName deletion failed:\n" +
                        "${e.message}")
            }

            instances.remove(instanceName)
            return HttpStatusCode.OK.description("Instance $instanceName deleted")
        }
    }
}