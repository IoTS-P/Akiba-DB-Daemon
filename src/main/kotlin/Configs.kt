package org.iotsplab.akiba.dbDaemon

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
    // Postgres instance directory
    val instanceRoot: String = "psql_instances",
    // The JSON file containing local instance information
    // NOTE: You CANNOT specify '~/.akiba/instances.json' which will be regarded as a relative path!
    val instanceMapFile: String = System.getProperty("user.home") + "/.akiba/instances.json",

    // Credential store for /instance/login, /instance/register and /instance/list.
    // There is deliberately NO default account: the first login against an empty
    // store registers itself as the administrator (see `UserStore`).
    val usersFile: String = System.getProperty("user.home") + "/.akiba/users.json",

    // ---------------------------------------------------------------------
    //  Bookkeeping database used for the backup tree (`db_backup_tree`) and
    //  the backup repository metadata (`akiba_backup`).  Both live in the
    //  *system* PostgreSQL cluster (the one installed by the OS/Docker
    //  image), NOT in the per-user instances.
    //
    //  `backupDbPassword` intentionally defaults to the empty string: it is
    //  read from the `AKIBA_DB_PASSWORD` environment variable when unset, and
    //  the daemon refuses backup operations when neither is provided.  There
    //  is no hard-coded fallback password.
    // ---------------------------------------------------------------------
    val backupDbHost: String = "localhost",
    val backupDbPort: Int = 5432,
    val backupDbUser: String = "akiba",
    val backupDbPassword: String = "",
    val backupDbName: String = "akiba_backup",
    val backupTreeDbName: String = "db_backup_tree",
) {
    /** Resolved password for the bookkeeping databases. Never a built-in default. */
    val resolvedBackupDbPassword: String
        get() = backupDbPassword.ifBlank { System.getenv("AKIBA_DB_PASSWORD") ?: "" }
}