# Akiba Database Daemon

## 1. Configuration Files

### `config.json` Main Configuration File

**You need to fill in some required configurations in the main configuration file to make Akiba Database Daemon run properly.**

```text
{
  // Console and file log levels
  "consoleLogLevel": "INFO",
  "fileLogLevel": "DEBUG",
  // HTTP service port
  "serverPort": 31777,
  // PostgreSQL instance directory (one sub-directory per instance; ports are
  // allocated automatically starting at 31800)
  "instanceRoot": "/akiba/instances",
  // pgbackrest repository root
  "backupRoot": "/akiba/backups",
  // Instance map file (name -> port / owner / last restored backup)
  "instanceMapFile": "/home/akiba/.akiba/instances.json",
  // Credential store. When the file is missing or empty, the first successful
  // login registers itself as this daemon's first administrator (this keeps
  // deployments that predate authentication usable); afterwards every login
  // must match a stored bcrypt hash.
  "usersFile": "/home/akiba/.akiba/users.json",
  // Connection info of the backup bookkeeping database (it lives in the system
  // PostgreSQL cluster, not in a user instance). When `backupDbPassword` is
  // empty the daemon reads AKIBA_DB_PASSWORD; if both are empty it refuses to
  // run any backup/restore (there is no hard-coded fallback password).
  "backupDbHost": "localhost",
  "backupDbPort": 5432,
  "backupDbUser": "akiba",
  "backupDbPassword": "",
  "backupDbName": "akiba_backup",
  "backupTreeDbName": "db_backup_tree",
  // Lock / token timeouts (seconds)
  "maxLockWaitingTime": 3600,
  "maxTokenWaitingTime": 7200,
  // Maximum time a single database operation may block its caller (seconds)
  "dbOperationTimeoutSeconds": 120
}
```

**The daemon ships no account at all**: no default username and no default
password. Credentials are created through `/instance/register` (or by the very
first login when the store is empty) and stored as bcrypt hashes in `usersFile`.

### Database Files

You can use `subprojects/akiba_db_daemon/src/main/resources/database_init.sql` to create a database template that can be used directly. The meaning and function of all fields are defined in the comments in the database script file. In the default compiled database daemon compressed package, the `subprojects/akiba_db_daemon/src/main/resources/initialize_pg_instance.sh` script creates a single instance (initdb, write the pgbackrest configuration, create the database and run `database_init.sql`); the daemon invokes it from `/ws/instance/create`, and the instance's owner is the user who requested it.

Database initialization will create 4 data tables, 1 view, and 1 index:

- `binaries`: Original binary file information
- `processed_binaries`: Processed binary file information
- `results`: Global task data
- `db_backup_tree`: Backup tree (**implemented**; the table lives in the daemon's own bookkeeping database, created by `resources/backup_db_init.sql`, and records each backup's parent/child relation, alias and expiry state)
- `using_binaries`: View of actual used binary file information (i.e., if a file exists in `processed_binaries`, the processed file is used; otherwise, the original file is used)

In `subprojects/akiba_db_daemon/src/main/resources/database_init.sql`, there are definitions for the structure of the above 5 tables. After removing format checks, the corresponding Chinese comments are as follows:

```postgresql
-- binaries: Original binary file information
CREATE TABLE IF NOT EXISTS binaries (
    id              INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- Binary file ID, unique identifier
    original_path   TEXT NOT NULL,          -- Original file path at import time. All binary files will be renamed and copied to the specified directory after import
    checksum        TEXT,                   -- MD5 digest value of the original file
    size            INTEGER,                -- Size of the original file
    arch            TEXT,                   -- Processor architecture of the binary file obtained at import time
    format          TEXT,                   -- Binary file format, such as ELF, EXE, PE, Mach-O, etc.
   compiler_spec   TEXT                    -- Compiler specification, such as eabi, Visual Studio, etc.
);

-- processed_binaries: Processed binary file information. Some binary files will be processed (currently only includes removal of large chunks of \x00 from files), so these files need to be saved.
-- Note: Not all binary files will be processed, so this table may not contain all IDs
CREATE TABLE IF NOT EXISTS processed_binaries (
    id              INTEGER REFERENCES binaries (id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE,
    original_path   TEXT,                   -- Original file path after processing
    checksum        TEXT,                   -- MD5 digest value of the processed file
    size            INTEGER,                -- Size of the processed file
    load_properties JSONB,                  -- File fragment information corresponding to the original file. During processing, consecutive \x00 larger than 0x10000 bytes will be removed
    arch            TEXT,                   -- Processor architecture of the processed binary file
    format          TEXT,                   -- Binary file format, such as ELF, EXE, PE, Mach-O, etc.
   compiler_spec   TEXT                    -- Compiler specification, such as eabi, Visual Studio, etc.
);

-- Global analysis results
CREATE TABLE IF NOT EXISTS results (
    id              INTEGER REFERENCES binaries (id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE,
    err_msg         TEXT,          -- Global error information
   global_data     TEXT,          -- Global data generated during analysis
    FOREIGN KEY(id) REFERENCES binaries (id)
       ON DELETE CASCADE
       ON UPDATE CASCADE
);

-- Create index. Since `id` is unique in `results`, we can create an index for it to improve the performance of `JOIN` commands.
CREATE INDEX IF NOT EXISTS idx_results_id ON results(id);

-- using_binaries: Get view of actual used binary file information. If a file exists in `processed_binaries`, the processed file is used; otherwise, the original file is used
CREATE OR REPLACE VIEW using_binaries AS
SELECT COALESCE(b.id, a.id) AS id,
       COALESCE(b.original_path, a.original_path) AS original_path,
       COALESCE(b.size, a.size) AS size,
       COALESCE(b.arch, a.arch) AS arch,
       COALESCE(b.format, a.format) AS format,
       COALESCE(b.compiler_spec, a.compiler_spec) AS compiler_spec,
       COALESCE(b.checksum, a.checksum) AS checksum,
       b.load_properties AS load_properties
FROM binaries a
         LEFT JOIN processed_binaries b
                   ON a.id = b.id;

-- Create database backup tree table (this table is not yet implemented, reserved as empty)
CREATE TABLE IF NOT EXISTS db_backup_tree (
    backup_id      UUID PRIMARY KEY,                                    -- Unique ID of the snapshot
    parent_id      UUID REFERENCES db_backup_tree(backup_id),           -- ID of parent snapshot
    alias          TEXT UNIQUE,                                         -- Alias of the snapshot, used to specify in configuration files
    lsn_start      pg_lsn NOT NULL,                                     -- Starting log sequence number of the snapshot
    lsn_end        pg_lsn NOT NULL,                                     -- Ending log sequence number of the snapshot
    backup_type    TEXT CHECK (backup_type IN ('BASE', 'WAL_DELTA')),   -- Snapshot type, whether it is a root-level snapshot or an incremental snapshot
    physical_path  TEXT NOT NULL,                                       -- Physical path of the snapshot file
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()                   -- Creation time of the snapshot
);
```

## 2. HTTP Path Interface Access

The Akiba Database Daemon enables the HTTP service at 0.0.0.0:31777 by default. Currently, all database operations are accessed via POST method or WebSocket method, with request parameters sent in JSON format. In Akiba Framework, interface implementations for all paths have been completed.

### 2.1 Authentication and Authorization

Except for `/test`, `/instance/login` and `/instance/register`, every interface requires the
`Authorization` field in the HTTP Header, whose value is the UUID Token obtained during login.

**Login Process:**
1. Call `/instance/login` with a username and password to get a Token. The daemon verifies the
   password against the bcrypt hash in `usersFile`; an unknown username and a wrong password both
   return `401` and are deliberately indistinguishable.
   - If the credential store is empty (a fresh deployment, or one that upgraded from a version
     without authentication), this login registers the username as the daemon's first administrator.
2. Add `Authorization: <Token>` to the Header of subsequent requests
3. Call `/instance/logout` to log out

**Brute-force protection (the same policy the web login applies):**
- every rejected login — wrong password *or* unknown username — waits **3 seconds** before the
  `401` is sent, so the endpoint cannot serve as a fast password oracle;
- **5 consecutive failures within 24 hours** lock the account for **24 hours**; while locked, even
  the correct password gets `423 Locked` and the message states when it unlocks;
- `POST /instance/unlock` (administrator token required, body `{"username": "..."}`) lifts the lock
  early;
- **Exception: callers on the loopback interface (`127.0.0.1`) are neither delayed nor counted.**
  In a single-host deployment the only loopback client is the Akiba framework server, which relays
  credentials a human already entered at the web login — not an attacker. Without this exemption,
  anyone able to reach port 31777 could lock an account with five wrong guesses and thereby cut the
  deployment off from its own daemon. Remote (non-loopback) callers get the full policy.
- The failure count and the lock live in `usersFile` (`failedAttempts` / `lockedUntil`), so a restart
  does not clear a lock.

**Account management:**
- `POST /instance/register`: create a credential (username, password, admin flag). Creating a
  **plain** user needs no authorization — a new username owns no instance and `/instance/connect`
  enforces ownership, so it can reach no data. Marking a user as administrator requires an
  administrator token.
- `POST /instance/set-password`: change an existing credential's password (administrator token
  required). The framework calls it on an administrator password reset; without it the account could
  sign in but every instance operation would be rejected.
- `POST /instance/unlock`: lift an account's brute-force lock (administrator token required).
- `POST /instance/list`: returns the instances **owned by the caller** (name, port, whether it is
  running, last restored backup). The web UI uses it to answer "does this user have an instance yet?".
- `POST /instance/worker-grant`: mints a derived credential for the caller (plaintext returned once,
  12 hours by default). The framework uses it when spawning child processes (workflow runs, manual
  agents) so that the account password never has to be written into a generated config file.

**Owner isolation:** an instance belongs to the username that created it, and `/instance/connect`
only lets the owner connect to their own instances — everyone else gets `403`. This is what makes
"each user only sees their own instances" true.

---

### 2.2 Query Interfaces (Queries)

#### GET /test
Test interface for checking if the server is running normally.

**Response Example:**
```
The server works well
```

#### POST /test
Test interface for checking if the server is running normally (POST method).

**Response Example:**
```
The server works well
```

#### POST /get/id/sql
Get a list of binary file IDs through SQL statements.

**Note:** For security reasons, this interface is disabled by default.

**Request Parameters:**
```json
{
  "sql": "WHERE u.ID < 100"
}
```

**Response:** ID list
```json
[1, 2, 3, ...]
```

#### POST /get/metadata
Get metadata of a specified binary file.

**Request Parameters:** Binary file ID
```json
123
```

**Response:** Metadata object
```json
{
  "id": 123,
  "originalPath": "/path/to/binary",
  "arch": "ARM",
  "format": "ELF",
  "compilerSpec": "eabi",
  "checksum": "md5_hash",
  "processedPath": "/path/to/processed",
  "processedChecksum": "md5_hash",
  "loadProperties": [
    {
      "oldOffset": 0,
      "newOffset": 0,
      "length": 65536
    }
  ]
}
```

#### POST /get/module_data
Get module data from module table.

**Request Parameters:**
```json
{
  "tableName": "test_module_results",
  "id": 123,
  "columns": ["column1", "column2"]  // Optional, query all columns if not filled
}
```

**Response:**
```json
{
  "results": [
    {"name": "column1", "type": "int4", "value": 42},
    {"name": "column2", "type": "bool", "value": true}
  ]
}
```

---

### 2.3 Insertion Interfaces (Insertions)

#### POST /insert/check_md5
Check if MD5 value already exists in the database.

**Request Parameters:** MD5 string
```json
"d41d8cd98f00b204e9800998ecf8427e"
```

**Response:** Boolean value
```json
true
```

#### POST /insert/insert_bin
Insert binary file information into the database.

**Request Parameters:**
```json
{
  "originalPath": "/path/to/binary",
  "processedPath": "/path/to/processed",  // Optional
  "checksum": "md5_hash",
  "processedChecksum": "md5_hash",        // Optional
  "size": 1024,
  "processedSize": 512,                   // Optional
  "loadProperties": "[{\"oldOffset\":0,\"newOffset\":0,\"length\":1024}]",  // Optional
  "arch": "ARM",
  "format": "ELF",
  "compilerSpec": "eabi"
}
```

**Response:** Inserted file ID
```json
123
```

---

### 2.4 Module Management Interfaces (Modules)

#### POST /module/create_table
Create a module-specific data table.

**Request Parameters:**
```json
{
  "name": "test_module_results",
  "columns": {
    "function_number": "integer",
    "has_flag": "boolean",
    "result_text": "text"
  }
}
```

**Supported Data Types:**
- `integer`, `bigint`
- `double precision`
- `text`
- `timestamptz`
- `interval`
- `boolean`
- `jsonb`
- `bytea`

**Reserved Column Names (Cannot be used):**
- `id`, `start_timestamp`, `finish_timestamp`, `execute_time`, `err_msg`

**Response:** HTTP status code

#### POST /module/create_view
Create a database view.

**Request Parameters:**
```json
{
  "viewName": "my_view",
  "viewSQL": "SELECT * FROM binaries WHERE arch = 'ARM'",
  "overwrite": false
}
```

**Response:** HTTP status code

#### POST /module/lock_table
Lock a specified table to prevent concurrent access conflicts.

**Request Parameters:**
```json
{
  "tableName": "test_module_results"
}
```

**Response:** HTTP status code

#### POST /module/unlock_table
Unlock a locked table.

**Request Parameters:**
```json
{
  "tableName": "test_module_results"
}
```

**Response:** HTTP status code

#### POST /module/update
Update data in the module data table. (Note: For a file ID, you must first send a `/module/start` request to start the module task before updating)

**Request Parameters:**
```json
{
  "tableName": "test_module_results",
  "id": 123,
  "data": {
    "function_number": 42,
    "has_flag": true,
    "result_text": "success"
  }
}
```

**Response:** HTTP status code

#### POST /module/start
Mark task as started.

**Request Parameters:**
```json
{
  "tableName": "test_module_results",
  "id": 123
}
```

**Function:** Set `start_timestamp` to current time, clear `finish_timestamp`, `execute_time`, and `err_msg`

**Response:** HTTP status code

#### POST /module/finish
Mark task as finished.

**Request Parameters:**
```json
{
  "tableName": "test_module_results",
  "id": 123
}
```

**Function:** Set `finish_timestamp` to current time, calculate `execute_time = finish_timestamp - start_timestamp`

**Response:** HTTP status code

---

### 2.5 Control Interfaces (Controls)

#### POST /control/enable
Enable a specified HTTP route.

**Request Parameters:**
```json
{
  "route": "/get/id/sql"
}
```

**Response:** HTTP status code

#### POST /control/disable
Disable a specified HTTP route.

**Request Parameters:**
```json
{
  "route": "/get/id/sql"
}
```

**Response:** HTTP status code

#### POST /heartbeat
Heartbeat interface for keeping session active.

**Response:** HTTP 204 No Content

---

### 2.6 Instance Management Interfaces (PGInstances)

#### POST /instance/login
Client login to get authentication Token.

**Request Parameters:**
```json
{
  "username": "test",
  "password": "test123"
}
```

**Response:**
```json
{
  "token": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### POST /instance/logout
Client logout to release resources.

**Response:** HTTP status code

#### WebSocket /ws/instance/create
Create a new PostgreSQL instance (WebSocket interface).

**Request Message:**
```json
{
  "token": "550e8400-e29b-41d4-a716-446655440000",
  "instanceName": "my_instance"
}
```

**Response Message:**
```json
{
  "msg": "Instance my_instance created"
}
```

#### POST /instance/connect
Connect to a specified PostgreSQL instance. If the instance is not started, it will be started automatically.

**Request Parameters:**
```json
{
  "instanceName": "my_instance"
}
```

**Response:** HTTP status code

#### POST /instance/disconnect
Disconnect from a PostgreSQL instance (without shutting down the instance).

**Request Parameters:**
```json
{
  "instanceName": "my_instance"
}
```

**Response:** HTTP status code

#### POST /instance/shutdown
Shut down a PostgreSQL instance.

**Request Parameters:**
```json
{
  "instanceName": "my_instance"
}
```

**Response:** HTTP status code

#### POST /instance/delete
Delete a PostgreSQL instance.

**Request Parameters:**
```json
{
  "instanceName": "my_instance"
}
```

**Response:** HTTP status code

---

### 2.7 Backup Management Interfaces (Backups)

#### WebSocket /ws/backup/create
Create a database backup (WebSocket interface).

**Request Message:**
```json
{
  "isFull": true,
  "token": "550e8400-e29b-41d4-a716-446655440000",
  "instance": "my_instance",
  "alias": "backup_2025",        // Optional
  "description": "Full backup"   // Optional
}
```

**Response Message:**
```json
{
  "label": "backup_label_123"
}
```

**Concurrency (important):** backups and restores of one instance are
**serialized**. If a backup or restore of that instance is already running, the
second request is refused immediately (websocket close code `4409`, which the
framework maps to HTTP `409`) instead of starting a second pgBackRest — which
would die on pgBackRest's repository lock with
`ERROR: [050]: unable to acquire lock on file '/tmp/pgbackrest/<stanza>-backup-1.lock'`
and reach the caller as a meaningless 500. A first full backup can take a minute,
so retry once it reports completion.

#### POST /backup/peek
View backup tree information of an instance.

**Request Parameters:** Instance name
```json
"my_instance"
```

**Response:** JSON representation of backup tree

#### POST /backup/status
Report whether a backup or restore of an instance (or of every instance the caller
owns) is running right now.

**Request:** `{"instance": "my_instance"}`, or `{}` for every instance of the caller.

**Response:**
```json
{ "busyInstances": ["my_instance"], "busy": true }
```

`busyInstances` lists the caller's instances with a run in flight; asking about one
instance also returns `busy`. The framework derives `backupInProgress` on
`GET /api/instances` and `busy` on `GET /api/instances/backups` from it, and the UI
**disables** the backup button accordingly instead of letting the click be refused.

#### WebSocket /ws/backup/restore
Restore database from backup (WebSocket interface).

**Request Message:**
```json
{
  "token": "550e8400-e29b-41d4-a716-446655440000",
  "instance": "my_instance",
  "aliasOrLabel": "backup_2025"
}
```

**Response Message:**
```json
{
  "msg": "Backup restoration completed"
}
```

---

### 2.8 Error Code Description

| HTTP Status Code | Meaning | Common Scenarios |
|-----------------|---------|-----------------|
| 200 OK | Request successful | Operation completed successfully |
| 204 No Content | Request successful but no content returned | Heartbeat interface |
| 400 Bad Request | Request parameter error | Incorrect parameter format, invalid Token |
| 401 Unauthorized | Unauthorized | Missing Authorization header |
| 404 Not Found | Resource does not exist | Table not found, instance not found|
| 409 Conflict | Resource conflict | Table already locked, instance already occupied |
| 423 Locked | Resource locked | Table locked by other client |
| 500 Internal Server Error | Server internal error | Database operation failed |

---

### 2.9 Usage Examples

#### Complete Process Example

```bash
# 1. Login to get TOKEN
TOKEN=$(curl -X POST http://localhost:31777/instance/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"test123"}' \
  | jq -r '.token')

# 2. Connect to instance
curl -X POST http://localhost:31777/instance/connect\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"instanceName":"my_instance"}'

# 3. Lock table
curl -X POST http://localhost:31777/module/lock_table\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results"}'

# 4. Create data table
curl -X POST http://localhost:31777/module/create_table\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"name":"test_module_results","columns":{"result":"integer"}}'

# 5. Start task
curl -X POST http://localhost:31777/module/start\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results","id":1}'

# 6. Update data
curl -X POST http://localhost:31777/module/update\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results","id":1,"data":{"result":42}}'

# 7. Finish task
curl -X POST http://localhost:31777/module/finish\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results","id":1}'

# 8. Unlock table
curl -X POST http://localhost:31777/module/unlock_table\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results"}'

# 9. Disconnect from instance
curl -X POST http://localhost:31777/instance/disconnect \
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"instanceName":"my_instance"}'

# 10. Logout
curl -X POST http://localhost:31777/instance/logout \
  -H "Authorization: $TOKEN"
```

---

**Important Notes:**
1. All interfaces requiring authentication must include a valid `Authorization` field in the HTTP Header
2. WebSocket interfaces require sending the authentication Token first after establishing connection
3. Tables must be locked before table operations and must be unlocked after completion
4. Instance operations require ensuring correct instance ownership
5. Backup operations take a long time; WebSocket interfaces are recommended for better progress feedback
