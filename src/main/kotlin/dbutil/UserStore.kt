package org.iotsplab.akiba.dbDaemon.dbutil

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.config
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger
import org.mindrot.jbcrypt.BCrypt
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermissions
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile

/**
 * Persistent credential store for the database daemon.
 *
 * **Why this exists**: `/instance/login` used to accept *any* username with
 * *any* password and hand out a token (see the old `// TODO: Add
 * authentications`). Everything else in the daemon is namespaced by that
 * username — instance ownership, instance listing, backup access — so the
 * missing check meant "whoever can reach the daemon port owns every
 * instance". This store closes that hole.
 *
 * **Storage**: a single JSON file (`Configs.usersFile`, default
 * `~/.akiba/users.json`) mapping username → bcrypt hash + admin flag. Bcrypt
 * is used through the same `org.mindrot:jbcrypt` library the framework
 * already ships, so the two components hash identically.
 *
 * **Bootstrap**: on a deployment that has never had a credential store (or
 * any user), the *first* successful login **registers** that username with
 * the password supplied, and marks it as an administrator. This keeps
 * existing deployments (whose data is already owned by some username) usable
 * without an out-of-band migration step. From then on, every login must
 * match a stored hash.
 *
 * Note that the framework's HTTP server only ever calls [login] with
 * credentials it has *already* verified against its own user table, so the
 * bootstrap path can never be triggered by an unauthenticated web request.
 */
object UserStore {

    /** Usernames end up as PostgreSQL role names, so keep them conservative. */
    const val USERNAME_PATTERN = "^[A-Za-z0-9._-]{1,64}$"

    /**
     * Brute-force policy for [login], mirroring the framework's web login
     * (`LoginProtection`): the same credentials are accepted by both, so a guess
     * must cost the same at either door.
     */
    const val MAX_LOGIN_FAILED_ATTEMPTS: Int = 5
    val LOGIN_FAILURE_WINDOW: java.time.Duration = java.time.Duration.ofHours(24)
    val LOGIN_LOCK_DURATION: java.time.Duration = java.time.Duration.ofHours(24)

    /** How long a rejected login is held back before the client is answered. */
    val LOGIN_FAILURE_DELAY: java.time.Duration = java.time.Duration.ofSeconds(3)
    const val MIN_PASSWORD_LENGTH = 6

    /** How many live derived (per-worker) secrets a single user may hold. */
    const val MAX_DERIVED_SECRETS = 8

    data class DerivedSecret(
        @JsonProperty("hash") var hash: String = "",
        @JsonProperty("label") var label: String = "",
        @JsonProperty("createdAt") var createdAt: String = "",
        /** ISO-8601 instant; the secret is rejected once it is in the past. */
        @JsonProperty("expiresAt") var expiresAt: String = "",
    )

    data class UserRecord(
        @JsonProperty("hash") var hash: String = "",
        @JsonProperty("isAdmin") var isAdmin: Boolean = false,
        @JsonProperty("createdAt") var createdAt: String = "",
        /**
         * Additional single-purpose credentials for this same user, minted by
         * [grantDerived] for child processes spawned by the framework. They are
         * verified exactly like [hash] but can expire on their own, so a leaked
         * generated config file does not expose the account password.
         */
        @JsonProperty("derived") var derived: MutableList<DerivedSecret> = mutableListOf(),
        /** Consecutive failed logins; reset by a successful login or an admin. */
        @JsonProperty("failedAttempts") var failedAttempts: Int = 0,
        /** ISO-8601 instant of the last failed login (empty when none). */
        @JsonProperty("lastFailedAt") var lastFailedAt: String = "",
        /**
         * ISO-8601 instant until which the account is locked (empty when it is
         * not). Mirrors the framework's web-login policy — see [login].
         */
        @JsonProperty("lockedUntil") var lockedUntil: String = "",
    )

    sealed class LoginResult {
        /** Credentials matched an existing record. */
        object Success : LoginResult()

        /** The store had no users at all, so this login created the first (admin) user. */
        object Bootstrapped : LoginResult()

        /** Unknown username, or wrong password. Deliberately not distinguished in responses. */
        object BadCredentials : LoginResult()

        /** Too many failures: the account is locked until [until] (ISO-8601). */
        class Locked(val until: String) : LoginResult()

        /** The credential file exists but cannot be read/parsed — fail closed. */
        class StoreUnavailable(val reason: String) : LoginResult()
    }

    enum class RegisterResult {
        Created,
        AlreadyExists,
        InvalidName,
        WeakPassword,
        NotAuthorized,
        StoreUnavailable,
    }

    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val lock = ReentrantLock()

    private var records: MutableMap<String, UserRecord>? = null
    private var loadError: String? = null

    private val storeFile: Path
        get() = Path.of(config.usersFile).toAbsolutePath()

    // ============================================================
    //  Loading / persisting
    // ============================================================

    private fun loadLocked(): MutableMap<String, UserRecord>? {
        records?.let { return it }
        if (loadError != null) return null

        val path = storeFile
        if (!path.exists()) {
            // No store yet — an empty store enables the bootstrap path.
            records = mutableMapOf()
            return records
        }
        if (!path.isRegularFile()) {
            loadError = "Credential store $path is not a regular file"
            globalLogger.error(loadError)
            return null
        }

        return try {
            val parsed: MutableMap<String, UserRecord> = mapper.readValue(path.toFile())
            records = parsed
            globalLogger.info("Loaded ${parsed.size} daemon credential(s) from $path")
            records
        } catch (e: Exception) {
            // Fail closed: an unreadable store must never degrade into "no users
            // configured", which would hand the next caller the bootstrap path.
            loadError = "Failed to parse credential store $path: ${e.message}"
            globalLogger.error(loadError)
            null
        }
    }

    private fun persistLocked(map: Map<String, UserRecord>) {
        val path = storeFile
        path.parent?.createDirectories()
        val tmp = path.resolveSibling(path.fileName.toString() + ".tmp")
        mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), map)
        try {
            Files.setPosixFilePermissions(
                tmp, PosixFilePermissions.fromString("rw-------")
            )
        } catch (_: Exception) {
            // Non-POSIX filesystem — best effort.
        }
        try {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (_: Exception) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING)
        }
        globalLogger.info("Persisted ${map.size} daemon credential(s) to $path")
    }

    // ============================================================
    //  Queries
    // ============================================================

    /** True when the store has no users at all (bootstrap is still possible). */
    fun isEmpty(): Boolean = lock.withLock { loadLocked()?.isEmpty() ?: false }

    /** Whether the store could be read; false means every login will fail closed. */
    fun isAvailable(): Boolean = lock.withLock { loadLocked() != null }

    fun isAdmin(username: String): Boolean =
        lock.withLock { loadLocked()?.get(username)?.isAdmin ?: false }

    fun usernames(): List<String> = lock.withLock { loadLocked()?.keys?.sorted() ?: emptyList() }

    fun hasUser(username: String): Boolean = lock.withLock { loadLocked()?.containsKey(username) == true }

    // ============================================================
    //  Login / registration
    // ============================================================

    @Throws(IllegalArgumentException::class)
    fun login(username: String, password: String, enforceLock: Boolean = true): LoginResult {
        val name = username.trim()
        require(name.isNotEmpty()) { "Username must not be empty" }

        return lock.withLock {
            val map = loadLocked()
                ?: return@withLock LoginResult.StoreUnavailable(loadError ?: "credential store unavailable")

            if (map.isEmpty()) {
                // Bootstrap: the very first login of a fresh (or never-configured)
                // deployment registers itself as the administrator.
                map[name] = newRecord(password, isAdmin = true)
                persistLocked(map)
                globalLogger.info(
                    "Daemon credential store was empty: registered '$name' as the first " +
                        "administrator account"
                )
                return@withLock LoginResult.Bootstrapped
            }

            val record = map[name] ?: return@withLock LoginResult.BadCredentials

            // A locked account is rejected before the password is even looked at.
            if (enforceLock) {
                lockExpiry(record)?.let { return@withLock LoginResult.Locked(it.toString()) }
            }

            val ok = try {
                if (BCrypt.checkpw(password, record.hash)) true
                else verifyDerived(name, record, password)
            } catch (e: Exception) {
                globalLogger.error("Failed to verify password for '$name': ${e.message}")
                false
            }

            if (ok) {
                if (record.failedAttempts != 0 || record.lockedUntil.isNotEmpty()) {
                    record.failedAttempts = 0
                    record.lastFailedAt = ""
                    record.lockedUntil = ""
                    persistLocked(map)
                }
                return@withLock LoginResult.Success
            }

            // A trusted (co-located) caller is not an attacker guessing: it only
            // ever relays credentials a human already entered at the web login, so
            // counting its failures would lock the deployment out of its own
            // daemon for a credential drift it cannot fix from here.
            if (!enforceLock) return@withLock LoginResult.BadCredentials

            // Failed login: count it against the failure window and lock once the
            // limit inside that window is reached.
            val now = OffsetDateTime.now()
            val withinWindow = runCatching { OffsetDateTime.parse(record.lastFailedAt) }
                .getOrNull()
                ?.isAfter(now.minus(LOGIN_FAILURE_WINDOW)) == true
            record.failedAttempts = if (withinWindow) record.failedAttempts + 1 else 1
            record.lastFailedAt = now.toString()
            if (record.failedAttempts >= MAX_LOGIN_FAILED_ATTEMPTS) {
                record.lockedUntil = now.plus(LOGIN_LOCK_DURATION).toString()
                globalLogger.warn(
                    "Daemon account '$name' locked until ${record.lockedUntil} after " +
                        "${record.failedAttempts} failed logins"
                )
            } else {
                globalLogger.info(
                    "Rejected daemon login for '$name' " +
                        "(${record.failedAttempts}/$MAX_LOGIN_FAILED_ATTEMPTS attempts)"
                )
            }
            persistLocked(map)

            lockExpiry(record)?.let { LoginResult.Locked(it.toString()) } ?: LoginResult.BadCredentials
        }
    }

    /** The instant until which [record] is locked, or null when it is not locked. */
    private fun lockExpiry(
        record: UserRecord,
        now: OffsetDateTime = OffsetDateTime.now(),
    ): OffsetDateTime? {
        if (record.lockedUntil.isEmpty()) return null
        val until = runCatching { OffsetDateTime.parse(record.lockedUntil) }.getOrNull() ?: return null
        return if (until.isAfter(now)) until else null
    }

    /**
     * Clear the failure counter and any lock of [username].
     *
     * Administrator action, mirroring the framework's `/admin/users/{id}/unlock`:
     * the lock would otherwise expire on its own after [LOGIN_LOCK_DURATION].
     *
     * @return false when there is no such user.
     */
    fun unlock(username: String): Boolean {
        val name = username.trim()
        return lock.withLock {
            val map = loadLocked() ?: return@withLock false
            val record = map[name] ?: return@withLock false
            if (record.failedAttempts == 0 && record.lockedUntil.isEmpty()) return@withLock true
            record.failedAttempts = 0
            record.lastFailedAt = ""
            record.lockedUntil = ""
            persistLocked(map)
            globalLogger.info("Daemon account '$name' unlocked by an administrator")
            true
        }
    }

    /**
     * Check [password] against the (non-expired) derived secrets of [record].
     * Expired secrets are dropped as a side effect; the caller holds the lock.
     */
    private fun verifyDerived(name: String, record: UserRecord, password: String): Boolean {
        val now = OffsetDateTime.now()
        val expired = record.derived.filter {
            runCatching { OffsetDateTime.parse(it.expiresAt).isBefore(now) }.getOrDefault(true)
        }
        if (expired.isNotEmpty()) {
            record.derived.removeAll(expired)
            globalLogger.debug("Pruned ${expired.size} expired derived credential(s) of '$name'")
        }
        val match = record.derived.any { BCrypt.checkpw(password, it.hash) }
        if (match) globalLogger.debug("'$name' authenticated with a derived credential")
        return match
    }

    /**
     * Mint a derived credential for [username] and return its plaintext.
     *
     * Used by the framework's HTTP server to hand spawned child processes an
     * identity without ever copying the account password into a generated config
     * file. The secret is stored hashed and expires after [ttlSeconds].
     *
     * @return the secret, or null when the user does not exist.
     */
    fun grantDerived(username: String, label: String, ttlSeconds: Long): String? {
        val name = username.trim()
        return lock.withLock {
            val map = loadLocked() ?: return@withLock null
            val record = map[name] ?: return@withLock null

            // Bound the number of live derived secrets: keep the newest few and
            // drop anything already expired, so a long-running server cannot grow
            // the file without limit.
            val now = OffsetDateTime.now()
            record.derived.removeAll { runCatching { OffsetDateTime.parse(it.expiresAt).isBefore(now) }
                .getOrDefault(true) }
            if (record.derived.size >= MAX_DERIVED_SECRETS) {
                record.derived.sortBy { it.createdAt }
                while (record.derived.size >= MAX_DERIVED_SECRETS) record.derived.removeAt(0)
            }

            val secret = "akw_" + UUID.randomUUID().toString().replace("-", "") +
                UUID.randomUUID().toString().replace("-", "")
            record.derived.add(
                DerivedSecret(
                    hash = BCrypt.hashpw(secret, BCrypt.gensalt()),
                    label = label,
                    createdAt = now.toString(),
                    expiresAt = now.plusSeconds(ttlSeconds).toString(),
                )
            )
            persistLocked(map)
            globalLogger.info(
                "Granted a derived credential to '$name' (label=$label, ttl=${ttlSeconds}s)")
            secret
        }
    }

    /**
     * Register a new credential.
     *
     * @param authorized whether the caller proved it may create users (i.e.
     *                   presented an administrator token). Creating the very
     *                   first user needs no authorization, because there is
     *                   nothing to protect yet.
     */
    @Throws(IllegalArgumentException::class)
    fun register(
        username: String,
        password: String,
        isAdmin: Boolean,
        authorized: Boolean,
    ): RegisterResult {
        val name = username.trim()
        if (!name.matches(Regex(USERNAME_PATTERN))) return RegisterResult.InvalidName
        if (password.length < MIN_PASSWORD_LENGTH) return RegisterResult.WeakPassword

        return lock.withLock {
            val map = loadLocked() ?: return@withLock RegisterResult.StoreUnavailable
            if (map.containsKey(name)) return@withLock RegisterResult.AlreadyExists
            // Creating a *plain* user is allowed without a token: a fresh username
            // owns no instance, and `/instance/connect` enforces ownership, so it
            // can reach no data. Creating an *administrator* does require one,
            // except for the very first account of an empty store.
            if (isAdmin && map.isNotEmpty() && !authorized)
                return@withLock RegisterResult.NotAuthorized

            val effectiveAdmin = isAdmin || map.isEmpty()
            map[name] = newRecord(password, effectiveAdmin)
            persistLocked(map)
            globalLogger.info(
                "Registered daemon credential '$name' (isAdmin=$effectiveAdmin, " +
                    "authorized=$authorized)"
            )
            RegisterResult.Created
        }
    }

    /**
     * Set a new password for an existing user. Used to keep the daemon's copy
     * in sync when the framework changes a user's password.
     *
     * @return false when the user does not exist (or the store is unreadable).
     */
    fun setPassword(username: String, password: String): Boolean {
        val name = username.trim()
        return lock.withLock {
            val map = loadLocked() ?: return@withLock false
            val record = map[name] ?: return@withLock false
            record.hash = BCrypt.hashpw(password, BCrypt.gensalt())
            persistLocked(map)
            globalLogger.info("Updated daemon password for '$name'")
            true
        }
    }

    /** Remove a credential. The last remaining administrator cannot be removed. */
    fun remove(username: String): Boolean {
        val name = username.trim()
        return lock.withLock {
            val map = loadLocked() ?: return@withLock false
            val record = map[name] ?: return@withLock false
            if (record.isAdmin && map.values.count { it.isAdmin } <= 1) return@withLock false
            map.remove(name)
            persistLocked(map)
            globalLogger.info("Removed daemon credential '$name'")
            true
        }
    }

    /** Drop every cached record so the next call re-reads the file (tests only). */
    fun reload() {
        lock.withLock {
            records = null
            loadError = null
        }
    }

    private fun newRecord(password: String, isAdmin: Boolean) = UserRecord(
        hash = BCrypt.hashpw(password, BCrypt.gensalt()),
        isAdmin = isAdmin,
        createdAt = OffsetDateTime.now().toString(),
    )
}
