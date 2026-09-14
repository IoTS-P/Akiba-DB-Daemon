# Akiba 数据库守护程序 （Akiba Database Daemon）

## 1. 配置文件

### `config.json` 主配置文件

**你需要填充主配置文件中必须填充的一些配置，以使 Akiba Database Daemon 正常运行。**

```text
{
  // 控制台和文件日志的级别
  "consoleLogLevel": "INFO",
  "fileLogLevel": "DEBUG",
  // HTTP 服务端口
  "serverPort": 31777,
  // PostgreSQL 实例目录（每个实例一个子目录，端口从 31800 起自动分配）
  "instanceRoot": "/akiba/instances",
  // pgbackrest 备份仓库根目录
  "backupRoot": "/akiba/backups",
  // 实例信息文件（实例名 -> 端口/属主/最近恢复的备份）
  "instanceMapFile": "/home/akiba/.akiba/instances.json",
  // 凭据库。文件不存在或为空时，第一次成功登录会把自己注册为该守护程序的
  // 第一个管理员（用于兼容升级前已有的部署）；此后每次登录都必须匹配。
  "usersFile": "/home/akiba/.akiba/users.json",
  // 备份记录库（位于系统 PostgreSQL，不是用户实例）的连接信息。
  // backupDbPassword 留空时读取环境变量 AKIBA_DB_PASSWORD；两者都为空时
  // 守护程序拒绝执行任何备份/恢复操作（不再有硬编码的默认口令）。
  "backupDbHost": "localhost",
  "backupDbPort": 5432,
  "backupDbUser": "akiba",
  "backupDbPassword": "",
  "backupDbName": "akiba_backup",
  "backupTreeDbName": "db_backup_tree",
  // 锁/令牌超时（秒）
  "maxLockWaitingTime": 3600,
  "maxTokenWaitingTime": 7200,
  // 单次数据库操作最长阻塞时间（秒），防止卡死的查询占住 Netty 线程
  "dbOperationTimeoutSeconds": 120
}
```

**守护程序不内置任何账号**：既没有默认用户名，也没有默认口令。凭据由
`/instance/register` 建立（或由用户库为空时的首次登录自动建立），口令以
bcrypt 哈希保存在 `usersFile` 中。

### 数据库文件

可以使用 `subprojects/akiba_db_daemon/src/main/resources/database_init.sql` 创建一个可以直接使用的数据库模板。所有字段的含义与功能在数据库脚本文件中的注释中定义。在默认编译的数据库守护程序压缩包中，`subprojects/akiba_db_daemon/src/main/resources/initialize_pg_instance.sh` 脚本负责创建单个实例（initdb、写 pgbackrest 配置、建库并执行 `database_init.sql`），由守护程序在 `/ws/instance/create` 中调用；实例的属主即发起创建的用户。

数据库初始化会创建 4 个数据表、1 个视图、1 个索引：

- `binaries`: 原始二进制文件信息
- `processed_binaries`: 处理过的二进制文件信息
- `results`: 全局任务数据
- `db_backup_tree`: 备份树（**已实现**；该表位于守护程序自己的记录库中，由 `resources/backup_db_init.sql` 创建，记录每次备份的父子关系、别名与过期状态）
- `using_binaries`: 实际使用的二进制文件信息视图（即如果一个文件在 `processed_binaries` 中存在，则使用处理后的文件，否则使用原始文件）

在 `subprojects/akiba_db_daemon/src/main/resources/database_init.sql` 中，有上述 5 个表结构的定义。去除格式检查后，对应的中文注释如下：

```postgresql
-- binaries: 原始二进制文件信息
CREATE TABLE IF NOT EXISTS binaries (
    id              INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- 二进制文件 ID，唯一标识符
    original_path   TEXT NOT NULL,          -- 导入时的文件原路径，所有二进制文件都将在导入后被重命名并统一复制到指定目录
    checksum        TEXT,                   -- 原文件的 MD5 摘要值
    size            INTEGER,                -- 原文件大小
    arch            TEXT,                   -- 导入时获取的二进制文件的处理器架构
    format          TEXT,                   -- 二进制文件格式，如 ELF，EXE，PE，Mach-O 等
    compiler_spec   TEXT                    -- 编译器规范，如 eabi，Visual Studio 等
);

-- processed_binaries: 处理过的二进制文件信息。一些二进制文件会被处理（目前仅包含针对文件的大块\x00减除），因此需要保存这些文件。
-- 注意：不是所有的二进制文件都会被处理，因此该表可能不包含所有 ID
CREATE TABLE IF NOT EXISTS processed_binaries (
    id              INTEGER REFERENCES binaries (id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE,
    original_path   TEXT,                   -- 处理后的文件原路径
    checksum        TEXT,                   -- 处理后文件的 MD5 摘要值
    size            INTEGER,                -- 处理后的文件大小
    load_properties JSONB,                  -- 文件片段信息，可与原文件进行对应。在处理过程中，大于 0x10000 字节的连续 \x00 会被减除
    arch            TEXT,                   -- 处理后的二进制文件的处理器架构
    format          TEXT,                   -- 二进制文件格式，如 ELF，EXE，PE，Mach-O 等
    compiler_spec   TEXT                    -- 编译器规范，如 eabi，Visual Studio 等
);

-- 全局分析结果
CREATE TABLE IF NOT EXISTS results (
    id              INTEGER REFERENCES binaries (id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE,
    err_msg         TEXT,          -- 全局错误信息
    global_data     TEXT,          -- 在分析时生成的全局数据
    FOREIGN KEY(id) REFERENCES binaries (id)
       ON DELETE CASCADE
       ON UPDATE CASCADE
);

-- 创建索引。由于`id` 在 `results` 中是唯一的，因此我们可以为它创建索引以提高 `JOIN` 命令的性能。
CREATE INDEX IF NOT EXISTS idx_results_id ON results(id);

-- using_binaries: 获取实际使用的二进制文件信息视图。如果一个文件在 `processed_binaries` 中存在，则使用处理后的文件，否则使用原始文件
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

-- 创建数据库备份树表（该表目前未实现，保留为空）
CREATE TABLE IF NOT EXISTS db_backup_tree (
    backup_id      UUID PRIMARY KEY,                                    -- 快照的独有 ID
    parent_id      UUID REFERENCES db_backup_tree(backup_id),           -- 父快照的 ID
    alias          TEXT UNIQUE,                                         -- 快照的别名，用于在配置文件中指定
    lsn_start      pg_lsn NOT NULL,                                     -- 快照的起始日志序列号
    lsn_end        pg_lsn NOT NULL,                                     -- 快照的结束日志序列号
    backup_type    TEXT CHECK (backup_type IN ('BASE', 'WAL_DELTA')),   -- 快照类型，是根级快照还是增量快照
    physical_path  TEXT NOT NULL,                                       -- 快照文件的物理路径
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()                   -- 快照的创建时间
);
```

## 2. HTTP 路径接口访问

Akiba 数据库守护程序默认开启 0.0.0.0:31777 的 HTTP 服务。目前所有数据库操作均通过 POST 方法或 WebSocket 方法访问，请求参数通过 JSON 格式发送。在 Akiba Framework 中，已经完成对所有路径的接口实现。

### 2.1 认证与授权

除 `/test` 与 `/instance/login`、`/instance/register` 外，所有接口都必须在 HTTP Header 中携带
`Authorization` 字段，其值为登录时获取的 UUID Token。

**登录流程：**
1. 调用 `/instance/login` 接口，携带用户名与口令，获取 Token。守护程序会用
   `usersFile` 中的 bcrypt 哈希校验口令：用户名不存在与口令错误都返回 `401`，不做区分。
   - 若凭据库为空（全新部署，或升级前从未配置过凭据的部署），本次登录会把该用户名
     注册为守护程序的第一个管理员；
2. 在后续请求的 Header 中添加 `Authorization: <Token>`
3. 调用 `/instance/logout` 退出登录

**防爆破（与前端登录同一套策略）：**
- 每次登录失败（口令错误，或用户名不存在）都会**先等待 3 秒**再返回 `401`，因此该接口
  无法被用作高速口令试探器；
- 同一账户在 **24 小时内连续失败 5 次**即被**锁定 24 小时**，锁定期内即使口令正确也返回
  `423 Locked`，响应消息里给出解锁时间；
- `POST /instance/unlock`（需要管理员 Token）可提前解锁，请求体 `{"username": "..."}`；
- **例外：来自 loopback（`127.0.0.1`）的调用不受锁定与计数约束。** 同机部署时只有 Akiba
  框架服务器会从 loopback 访问守护程序，它转发的是用户在 Web 登录时已经输入过的凭据，
  并非攻击者。若不作此豁免，任何能访问 31777 端口的人都可以通过 5 次错误口令把某个账户
  锁死，从而让整个部署再也连不上自己的守护程序。远程（非 loopback）调用仍然受完整策略
  约束。
- 账户的失败计数与锁定状态保存在 `usersFile` 中（字段 `failedAttempts` / `lockedUntil`），
  因此重启不会清除锁定。

**账号管理：**
- `POST /instance/register`：创建新凭据（用户名、口令、是否管理员）。创建**普通**用户
  不需要授权：新用户名不拥有任何实例，而 `/instance/connect` 会校验属主，因此它拿不到
  任何数据；把用户标记为管理员则需要一个管理员 Token。
- `POST /instance/set-password`：修改已有凭据的口令（需要管理员 Token）。框架的管理员改密
  会调用它，否则会出现“能登录但所有实例操作 401”。
- `POST /instance/unlock`：解除某账户的防爆破锁定（需要管理员 Token）。
- `POST /instance/list`：返回**当前用户拥有**的实例列表（名称、端口、是否在运行、最近恢复的
  备份）。框架前端用它判断“该用户是否还没有任何实例”。
- `POST /instance/worker-grant`：为当前用户签发一个派生凭据（明文只返回一次，默认 12 小时
  过期）。框架在派生子进程（工作流、手动 Agent）时使用它，从而不必把账号口令写进生成的
  配置文件。

**属主隔离：** 实例属于创建它的用户名；`/instance/connect` 只允许属主连接自己的实例，
其余请求返回 `403`。这是“每个用户只能看到自己的实例”的基础。

---

### 2.2 查询接口 (Queries)

#### GET /test
测试接口，用于检查服务器是否正常运行。

**响应示例：**
```
The server works well
```

#### POST /test
测试接口，用于检查服务器是否正常运行（POST 方式）。

**响应示例：**
```
The server works well
```

#### POST /get/id/sql
通过 SQL 语句获取二进制文件 ID 列表。

**注意：** 由于安全原因，该接口默认禁用。

**请求参数：**
```json
{
  "sql": "WHERE u.ID < 100"
}
```

**响应：** ID 列表
```json
[1, 2, 3, ...]
```

#### POST /get/metadata
获取指定二进制文件的元数据。

**请求参数：** 二进制文件 ID
```json
123
```

**响应：** 元数据对象
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
从模块表中获取模块数据。

**请求参数：**
```json
{
  "tableName": "test_module_results",
  "id": 123,
  "columns": ["column1", "column2"]  // 可选，不填则查询所有列
}
```

**响应：**
```json
{
  "results": [
    {"name": "column1", "type": "int4", "value": 42},
    {"name": "column2", "type": "bool", "value": true}
  ]
}
```

---

### 2.3 插入接口 (Insertions)

#### POST /insert/check_md5
检查 MD5 值是否已存在于数据库中。

**请求参数：** MD5 字符串
```json
"d41d8cd98f00b204e9800998ecf8427e"
```

**响应：** 布尔值
```json
true
```

#### POST /insert/insert_bin
插入二进制文件信息到数据库。

**请求参数：**
```json
{
  "originalPath": "/path/to/binary",
  "processedPath": "/path/to/processed",  // 可选
  "checksum": "md5_hash",
  "processedChecksum": "md5_hash",        // 可选
  "size": 1024,
  "processedSize": 512,                   // 可选
  "loadProperties": "[{\"oldOffset\":0,\"newOffset\":0,\"length\":1024}]",  // 可选
  "arch": "ARM",
  "format": "ELF",
  "compilerSpec": "eabi"
}
```

**响应：** 插入的文件 ID
```json
123
```

---

### 2.4 模块管理接口 (Modules)

#### POST /module/create_table
创建模块专用数据表。

**请求参数：**
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

**支持的数据类型：**
- `integer`, `bigint`
- `double precision`
- `text`
- `timestamptz`
- `interval`
- `boolean`
- `jsonb`
- `bytea`

**保留列名（不可使用）：**
- `id`, `start_timestamp`, `finish_timestamp`, `execute_time`, `err_msg`

**响应：** HTTP 状态码

#### POST /module/create_view
创建数据库视图。

**请求参数：**
```json
{
  "viewName": "my_view",
  "viewSQL": "SELECT * FROM binaries WHERE arch = 'ARM'",
  "overwrite": false
}
```

**响应：** HTTP 状态码

#### POST /module/lock_table
锁定指定表，防止并发访问冲突。

**请求参数：**
```json
{
  "tableName": "test_module_results"
}
```

**响应：** HTTP 状态码

#### POST /module/unlock_table
解锁已锁定的表。

**请求参数：**
```json
{
  "tableName": "test_module_results"
}
```

**响应：** HTTP 状态码

#### POST /module/update
更新模块数据表中的数据。（注意：对于一个文件 id，必须首先发起 `/module/start` 请求启动该模块任务后，才能进行更新操作）

**请求参数：**
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

**响应：** HTTP 状态码

#### POST /module/start
标记任务开始执行。

**请求参数：**
```json
{
  "tableName": "test_module_results",
  "id": 123
}
```

**功能：** 设置 `start_timestamp` 为当前时间，清除 `finish_timestamp`、`execute_time` 和 `err_msg`

**响应：** HTTP 状态码

#### POST /module/finish
标记任务执行完成。

**请求参数：**
```json
{
  "tableName": "test_module_results",
  "id": 123
}
```

**功能：** 设置 `finish_timestamp` 为当前时间，计算 `execute_time = finish_timestamp - start_timestamp`

**响应：** HTTP 状态码

---

### 2.5 控制接口 (Controls)

#### POST /control/enable
启用指定的 HTTP 路由。

**请求参数：**
```json
{
  "route": "/get/id/sql"
}
```

**响应：** HTTP 状态码

#### POST /control/disable
禁用指定的 HTTP 路由。

**请求参数：**
```json
{
  "route": "/get/id/sql"
}
```

**响应：** HTTP 状态码

#### POST /heartbeat
心跳接口，用于保持会话活跃。

**响应：** HTTP 204 No Content

---

### 2.6 实例管理接口 (PGInstances)

#### POST /instance/login
客户端登录，获取认证 Token。

**请求参数：**
```json
{
  "username": "test",
  "password": "test123"
}
```

**响应：**
```json
{
  "token": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### POST /instance/logout
客户端登出，释放资源。

**响应：** HTTP 状态码

#### WebSocket /ws/instance/create
创建新的 PostgreSQL 实例（WebSocket接口）。

**请求消息：**
```json
{
  "token": "550e8400-e29b-41d4-a716-446655440000",
  "instanceName": "my_instance"
}
```

**响应消息：**
```json
{
  "msg": "Instance my_instance created"
}
```

#### POST /instance/connect
连接到指定的 PostgreSQL 实例。如果实例未启动，会自动启动。

**请求参数：**
```json
{
  "instanceName": "my_instance"
}
```

**响应：** HTTP 状态码

#### POST /instance/disconnect
断开与 PostgreSQL 实例的连接（不关闭实例）。

**请求参数：**
```json
{
  "instanceName": "my_instance"
}
```

**响应：** HTTP 状态码

#### POST /instance/shutdown
关闭 PostgreSQL 实例。

**请求参数：**
```json
{
  "instanceName": "my_instance"
}
```

**响应：** HTTP 状态码

#### POST /instance/delete
删除 PostgreSQL 实例。

**请求参数：**
```json
{
  "instanceName": "my_instance"
}
```

**响应：** HTTP 状态码

---

### 2.7 备份管理接口 (Backups)

#### WebSocket /ws/backup/create
创建数据库备份（WebSocket接口）。

**请求消息：**
```json
{
  "isFull": true,
  "token": "550e8400-e29b-41d4-a716-446655440000",
  "instance": "my_instance",
  "alias": "backup_2025",        // 可选
  "description": "Full backup"   // 可选
}
```

**响应消息：**
```json
{
  "label": "backup_label_123"
}
```

**并发约束（重要）：** 同一实例的备份/恢复是**串行**的。若该实例已有一次备份或恢复在运行，
第二个请求会**立即**收到冲突应答（WebSocket 关闭码 `4409`，框架层对应 HTTP `409`），
而不会去并发启动 pgBackRest —— 否则第二个进程会撞上 pgBackRest 的仓库锁并报
`ERROR: [050]: unable to acquire lock on file '/tmp/pgbackrest/<stanza>-backup-1.lock'`，
调用方只能看到一个无意义的 500。首个全量备份可能耗时一分钟，请在收到失败应答后重试。

#### POST /backup/peek
查看实例的备份树信息。

**请求参数：** 实例名称
```json
"my_instance"
```

**响应：** 备份树的 JSON 表示

#### POST /backup/status
查询某个实例（或当前用户的全部实例）此刻是否有备份/恢复在运行。

**请求参数：** `{"instance": "my_instance"}`；`{}` 表示查询当前用户的所有实例。

**响应：**
```json
{ "busyInstances": ["my_instance"], "busy": true }
```

`busyInstances` 是当前用户拥有的、正在备份/恢复的实例名列表；传入具体实例时额外返回 `busy`。
框架的 `GET /api/instances`（字段 `backupInProgress`）与 `GET /api/instances/backups`
（字段 `busy`）都由它派生，前端据此**禁用**备份按钮，避免用户点到被拒绝的请求。

#### WebSocket /ws/backup/restore
从备份恢复数据库（WebSocket接口）。

**请求消息：**
```json
{
  "token": "550e8400-e29b-41d4-a716-446655440000",
  "instance": "my_instance",
  "aliasOrLabel": "backup_2025"
}
```

**响应消息：**
```json
{
  "msg": "Backup restoration completed"
}
```

---

### 2.8 错误码说明

| HTTP 状态码 | 含义 | 常见场景 |
|------------|------|----------|
| 200 OK | 请求成功 | 操作成功完成 |
| 204 No Content | 请求成功但无返回内容 | 心跳接口 |
| 400 Bad Request | 请求参数错误 | 参数格式不正确、Token 无效 |
| 401 Unauthorized | 未授权 | 缺少 Authorization 头 |
| 404 Not Found| 资源不存在 | 表不存在、实例不存在 |
| 409 Conflict | 资源冲突 | 表已锁定、实例已被占用 |
| 423 Locked | 资源被锁定 | 表被其他客户端锁定 |
| 500 Internal Server Error | 服务器内部错误 | 数据库操作失败 |

---

### 2.9 使用示例

#### 完整流程示例

```bash
# 1. 登录获取 TOKEN
TOKEN=$(curl -X POST http://localhost:31777/instance/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"test123"}' \
  | jq -r '.token')

# 2. 连接实例
curl -X POST http://localhost:31777/instance/connect \
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"instanceName":"my_instance"}'

# 3. 锁定表
curl -X POST http://localhost:31777/module/lock_table\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results"}'

# 4. 创建数据表
curl -X POST http://localhost:31777/module/create_table\
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"name":"test_module_results","columns":{"result":"integer"}}'

# 5. 开始任务
curl -X POST http://localhost:31777/module/start \
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results","id":1}'

# 6. 更新数据
curl -X POST http://localhost:31777/module/update \
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results","id":1,"data":{"result":42}}'

# 7. 完成任务
curl -X POST http://localhost:31777/module/finish \
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results","id":1}'

# 8. 解锁表
curl -X POST http://localhost:31777/module/unlock_table \
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"tableName":"test_module_results"}'

# 9. 断开连接
curl -X POST http://localhost:31777/instance/disconnect \
  -H "Content-Type: application/json" \
  -H "Authorization: $TOKEN" \
  -d '{"instanceName":"my_instance"}'

# 10. 登出
curl -X POST http://localhost:31777/instance/logout \
  -H "Authorization: $TOKEN"
```

---

**注意事项：**
1. 所有需要认证的接口必须在 HTTP Header 中包含有效的 `Authorization` 字段
2. WebSocket接口需要在建立连接后首先发送认证 Token
3. 表操作前必须先锁定表，操作完成后必须解锁
4. 实例操作需要确保实例的所有权正确
5. 备份操作耗时较长，建议使用 WebSocket接口以获得更好的进度反馈