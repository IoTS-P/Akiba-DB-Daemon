package org.iotsplab.akiba.dbDaemon.dbutil

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.config
import org.iotsplab.akiba.dbDaemon.DatabaseDaemon.Companion.globalLogger
import org.iotsplab.akiba.dbDaemon.operations.PGInstances
import java.nio.file.Files
import java.nio.file.Path
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.io.path.absolute
import kotlin.io.path.absolutePathString
import kotlin.io.path.createDirectories
import kotlin.io.path.exists

/**
 * Logical (per-database) backups of one instance.
 *
 * Instances are databases inside one cluster, so a physical copy of a data
 * directory would capture every instance at once. `pg_dump` in custom format is
 * the operation that matches the model: one file per backup, restored with
 * `pg_restore` into the instance database.
 *
 * Every backup is a full dump — incremental levels needed a cluster to diff
 * against. The manifest lives next to the files, so a backup set can be copied
 * or inspected without a database.
 */
class BackupManager (val instanceName: String) {

    /** One entry in the manifest, and one node in the list the UI renders. */
    data class BackupEntry(
        val label: String,
        val alias: String? = null,
        val description: String? = null,
        val createdAt: String,
        val sizeBytes: Long = 0,
    )

    data class BackupNode(
        val label: String,
        val isExpired: Boolean,
        val alias: String?,
        val description: String?,
        val parent: String?,
        val backupType: BackupType,
        val createdAt: OffsetDateTime,
        val children: MutableList<BackupNode> = mutableListOf(),
    )

    object BackupNodeSerializer: JsonSerializer<BackupNode>() {
        override fun serialize(
            value: BackupNode, gen: JsonGenerator, serializers: SerializerProvider
        ) {
            gen.writeStartObject()
            gen.writeStringField("label", value.label)
            gen.writeBooleanField("isExpired", value.isExpired)
            gen.writeStringField("alias", value.alias)
            gen.writeStringField("description", value.description)
            gen.writeStringField("parent", value.parent)
            gen.writeStringField("backupType", value.backupType.name)
            gen.writeStringField("createdAt", value.createdAt.toString())
            gen.writeArrayFieldStart("children")
            value.children.forEach { serialize(it, gen, serializers) }
            gen.writeEndArray()
            gen.writeEndObject()
        }
    }

    enum class BackupType { FULL, DIFF, INCR }

    private val backupDirectory: Path
        get() = Path.of(config.backupRoot, instanceName).absolute()

    private val manifestFile: Path
        get() = backupDirectory.resolve("manifest.json")

    /** Create a backup and return its label. */
    @Throws(BackupInProgressException::class, IllegalStateException::class)
    fun backup(alias: String? = null, description: String? = null, isFull: Boolean = false): String {
        val lock = lockFor(instanceName)
        if (!lock.tryLock()) throw BackupInProgressException(instanceName)
        try {
            backupDirectory.createDirectories()
            val label = nextLabel()
            val target = backupDirectory.resolve("$label.dump")

            run(
                command = listOf(
                    pgBin("pg_dump"), "-Fc", "--no-owner", "--no-privileges",
                    "-h", config.dbHost, "-p", config.dbPort.toString(),
                    "-U", config.dbUser, "-d", PGInstances.databaseName(instanceName),
                    "-f", target.absolutePathString(),
                ),
                operation = "FULL backup",
                target = target,
            )

            val entries = readManifest().toMutableList()
            entries.add(
                BackupEntry(
                    label = label,
                    alias = alias?.takeIf { it.isNotBlank() },
                    description = description?.takeIf { it.isNotBlank() },
                    createdAt = OffsetDateTime.now(ZoneOffset.UTC).toString(),
                    sizeBytes = Files.size(target),
                )
            )
            writeManifest(entries)
            globalLogger.info("Backup $label of instance $instanceName written to $target")
            return label
        } finally {
            lock.unlock()
        }
    }

    /** Restore the backup named by label or alias into the instance database. */
    @Throws(BackupInProgressException::class, IllegalStateException::class)
    fun restoreBackup(labelOrAlias: String) {
        val lock = lockFor(instanceName)
        if (!lock.tryLock()) throw BackupInProgressException(instanceName)
        try {
            val entry = readManifest().lastOrNull { it.label == labelOrAlias || it.alias == labelOrAlias }
                ?: throw IllegalStateException(
                    "No backup '$labelOrAlias' for instance $instanceName")
            val source = backupDirectory.resolve("${entry.label}.dump")
            if (!source.exists())
                throw IllegalStateException("Backup file of ${entry.label} is missing: $source")

            run(
                command = listOf(
                    pgBin("pg_restore"), "--clean", "--if-exists", "--no-owner",
                    "-h", config.dbHost, "-p", config.dbPort.toString(),
                    "-U", config.dbUser, "-d", PGInstances.databaseName(instanceName),
                    source.absolutePathString(),
                ),
                operation = "restore of ${entry.label}",
                target = null,
            )
            globalLogger.info("Restored ${entry.label} into instance $instanceName")
        } finally {
            lock.unlock()
        }
    }

    /** Backups of this instance, newest first, as flat nodes. */
    fun getLogicalData(): List<BackupNode> =
        readManifest()
            .sortedByDescending { it.createdAt }
            .map { entry ->
                BackupNode(
                    label = entry.label,
                    isExpired = false,
                    alias = entry.alias,
                    description = entry.description,
                    parent = null,
                    backupType = BackupType.FULL,
                    createdAt = runCatching { OffsetDateTime.parse(entry.createdAt) }
                        .getOrDefault(OffsetDateTime.now(ZoneOffset.UTC)),
                )
            }

    private fun pgBin(tool: String): String =
        Path.of(config.pgBinDirectory, tool).absolutePathString()

    /**
     * Run a pg tool and turn a non-zero exit into an exception carrying its
     * stderr, so the route can report why a backup failed.
     */
    private fun run(command: List<String>, operation: String, target: Path?) {
        val password = config.resolvedDbPassword
        if (password.isBlank())
            throw IllegalStateException(
                "No password for the database cluster. Set `dbPassword` in the daemon " +
                    "configuration or AKIBA_DB_PASSWORD in the environment."
            )

        val builder = ProcessBuilder(command).redirectErrorStream(true)
        // pg_dump/pg_restore read the password from the environment; writing it to
        // stdin would be ignored and they would block on a terminal prompt.
        builder.environment()["PGPASSWORD"] = password
        val process = builder.start()
        val output = process.inputStream.bufferedReader().readText()

        if (!process.waitFor(30, TimeUnit.MINUTES)) {
            process.destroyForcibly()
            target?.let { runCatching { Files.deleteIfExists(it) } }
            throw IllegalStateException("$operation timed out after 30 minutes")
        }
        if (process.exitValue() != 0) {
            target?.let { runCatching { Files.deleteIfExists(it) } }
            globalLogger.error("pg tool failed during $operation: $output")
            throw IllegalStateException(
                "$operation failed (exit=${process.exitValue()}): " +
                    output.lines().filter { it.isNotBlank() }.takeLast(3).joinToString(" | ")
            )
        }
    }

    private fun nextLabel(): String {
        val stamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
            .withZone(ZoneOffset.UTC)
            .format(java.time.Instant.now())
        var label = stamp
        var suffix = 1
        while (backupDirectory.resolve("$label.dump").exists()) {
            label = "$stamp-$suffix"
            suffix++
        }
        return label
    }

    private fun readManifest(): List<BackupEntry> {
        if (!manifestFile.exists()) return emptyList()
        return runCatching {
            jacksonObjectMapper().readValue<List<BackupEntry>>(manifestFile.toFile())
        }.getOrElse { e ->
            globalLogger.warn("Cannot read backup manifest of $instanceName: ${e.message}")
            emptyList()
        }
    }

    private fun writeManifest(entries: List<BackupEntry>) {
        backupDirectory.createDirectories()
        jacksonObjectMapper().writerWithDefaultPrettyPrinter()
            .writeValue(manifestFile.toFile(), entries)
    }

    companion object {
        /**
         * Process-wide backup/restore locks, keyed by instance name.
         *
         * Shared on purpose: callers create a new BackupManager per request, so a
         * per-object lock would not exclude anything — see [backup].
         */
        private val backupLocks = ConcurrentHashMap<String, ReentrantLock>()

        private fun lockFor(instanceName: String): ReentrantLock =
            backupLocks.computeIfAbsent(instanceName) { ReentrantLock() }

        /** Whether a backup or restore of [instanceName] is running right now. */
        fun isBackupRunning(instanceName: String): Boolean =
            backupLocks[instanceName]?.isLocked == true

        /** Instance names with a backup/restore in flight, for the UI to grey out. */
        fun busyInstances(): Set<String> =
            backupLocks.filterValues { it.isLocked }.keys.toSet()
    }
}


class BackupInProgressException(instanceName: String) : IllegalStateException(
    // Kept short: this text also travels as a websocket close reason, which
    // RFC 6455 caps at 123 bytes (see AbstractWebsocketRoute).
    "Another backup or restore of instance '$instanceName' is running; retry when it finishes."
)
