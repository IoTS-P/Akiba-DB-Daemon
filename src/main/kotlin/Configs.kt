package org.iotsplab.akiba.dbDaemon

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Daemon configuration.
 *
 * Unknown keys are ignored so a config file written for another build does not
 * stop the daemon from starting with an otherwise valid set of options.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class Configs (
    // Database log level
    val consoleLogLevel: String = "INFO",
    val fileLogLevel: String = "DEBUG",
    // Server port
    val serverPort: Int = 31777,
    // Maximum time to wait for a lock (seconds), default at 1 hour.
    // If the server didn't receive a response from a client within this time, it will release the lock
    val maxLockWaitingTime: Int = 3600,
    // Maximum time to wait for a token (seconds), default at 2 hours.
    // If the server didn't receive a response from a client within this time, it will release the token
    val maxTokenWaitingTime: Int = 7200,

    // Maximum time a single database operation may block its caller
    // (seconds), default 2 minutes.  Guards against a wedged per-session
    // DB worker (stuck query, or a task orphaned when the session is
    // closed mid-flight) parking the calling Netty event-loop thread
    // forever — which previously wedged the whole daemon.
    val dbOperationTimeoutSeconds: Int = 120,

    // Postgres bin directory, the directory that contains pg_basebackup, pg_dump, pg_restore, etc
    val pgBinDirectory: String = "/usr/bin",
    // Backup directory
    val backupRoot: String = "backups",
    // The JSON file containing local instance information
    // NOTE: you CANNOT specify '~/.akiba/instances.json' which will be regarded as a relative path!
    val instanceMapFile: String = System.getProperty("user.home") + "/.akiba/instances.json",

    // Credential store for /instance/login, /instance/register and /instance/list.
    // There is deliberately NO default account: the first login against an empty
    // store registers itself as the administrator (see `UserStore`).
    val usersFile: String = System.getProperty("user.home") + "/.akiba/users.json",

    // ---------------------------------------------------------------------
    //  The one PostgreSQL cluster Akiba uses.
    //
    //  Every instance is a database inside this cluster — `<prefix><name>` —
    //  rather than a cluster of its own, so there is no port allocation, no
    //  pg_ctl lifecycle and no per-instance data directory. Table locks and
    //  per-session connections still apply: they guard concurrent writers, not
    //  user isolation.
    //
    //  `dbPassword` defaults to the empty string on purpose: it is read from
    //  `AKIBA_DB_PASSWORD` when unset, and the daemon refuses to connect when
    //  neither is provided. There is no hard-coded fallback password.
    // ---------------------------------------------------------------------
    val dbHost: String = "localhost",
    val dbPort: Int = 5432,
    val dbUser: String = "akiba",
    val dbPassword: String = "",
    /** Instance databases are named `<prefix><instance>` to stay clear of system databases. */
    val instanceDatabasePrefix: String = "inst_",
    /**
     * Schemas applied to every new instance database, in order.
     *
     * `database_init.sql` creates the analysis tables, `agent_database_init.sql`
     * the agent session/memory tables — an instance without the second one cannot
     * open a chat, which is exactly what the old init script applied both for.
     */
    val instanceSchemaFiles: List<String> = listOf(
        "resources/database_init.sql",
        "resources/agent_database_init.sql",
    ),
    /** Bookkeeping database for the backup tree. */
    val backupDbName: String = "akiba_backup",
    val backupTreeDbName: String = "db_backup_tree",
) {
    /** Resolved password for the cluster. Never a built-in default. */
    val resolvedDbPassword: String
        get() = dbPassword.ifBlank { System.getenv("AKIBA_DB_PASSWORD") ?: "" }
}
