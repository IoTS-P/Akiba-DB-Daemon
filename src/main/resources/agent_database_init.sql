-- ============================================================
-- Akiba Agent Framework — Database Schema
-- ============================================================
-- This file is executed alongside database_init.sql when a new
-- PostgreSQL instance is created.  It defines all tables that
-- the Agent subsystem needs for session persistence, memory
-- storage, graph topology, and human-in-the-loop interaction.
--
-- Design principles:
--   • Idempotent (CREATE TABLE IF NOT EXISTS / ALTER TABLE … IF NOT EXISTS)
--   • Compatible with assert_column_type() defined in database_init.sql
--   • Column/table names follow Akiba naming convention: ^[a-z][a-z0-9_]{0,62}$
-- ============================================================

-- ----------------------------------------------------------
-- 1. agent_sessions
--    The top-level session record.  One session represents a
--    complete agent interaction lifecycle (from creation to
--    completion or suspension).
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_sessions (
    session_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_name    TEXT,                          -- optional human-readable label
    status          TEXT NOT NULL DEFAULT 'active'  -- active | suspended | completed | error
                        CHECK (status IN ('active','suspended','completed','error')),
    binary_id       INTEGER REFERENCES binaries(id)
                        ON DELETE SET NULL
                        ON UPDATE CASCADE,         -- the binary being analyzed (nullable for non-binary sessions)
    module_name     TEXT,                          -- the AkibaModule that owns this session
    graph_id        UUID,                          -- FK → agent_graphs (set after graph is bound)
    model_name      TEXT,                          -- e.g. "gpt-4o", "claude-3-sonnet"
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resumed_at      TIMESTAMPTZ,                   -- last time the session was resumed from suspension
    completed_at    TIMESTAMPTZ                    -- time the session reached terminal state
);

SELECT assert_column_type('public', 'agent_sessions', 'session_id',    'uuid');
SELECT assert_column_type('public', 'agent_sessions', 'session_name',  'text');
SELECT assert_column_type('public', 'agent_sessions', 'status',        'text');
SELECT assert_column_type('public', 'agent_sessions', 'binary_id',     'int4');
SELECT assert_column_type('public', 'agent_sessions', 'module_name',   'text');
SELECT assert_column_type('public', 'agent_sessions', 'graph_id',      'uuid');
SELECT assert_column_type('public', 'agent_sessions', 'model_name',    'text');
SELECT assert_column_type('public', 'agent_sessions', 'created_at',    'timestamptz');
SELECT assert_column_type('public', 'agent_sessions', 'updated_at',    'timestamptz');
SELECT assert_column_type('public', 'agent_sessions', 'resumed_at',    'timestamptz');
SELECT assert_column_type('public', 'agent_sessions', 'completed_at',  'timestamptz');

CREATE INDEX IF NOT EXISTS idx_agent_sessions_status    ON agent_sessions(status);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_binary_id ON agent_sessions(binary_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_module    ON agent_sessions(module_name);

-- ----------------------------------------------------------
-- 2. agent_messages
--    Stores the full conversation history (user / assistant /
--    system / tool messages) for every session.  This is the
--    backing store for langchain4j's ChatMemory.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_messages (
    message_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      UUID NOT NULL REFERENCES agent_sessions(session_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    message_index   INTEGER NOT NULL,              -- ordinal position within the session
    role            TEXT NOT NULL CHECK (role IN ('system','user','assistant','tool')),
    content         TEXT,                          -- main text content (nullable for tool calls)
    tool_call_id    TEXT,                          -- correlates a tool response to its call
    tool_name       TEXT,                          -- the name of the tool that was invoked
    tool_call_args  JSONB,                         -- arguments passed to the tool (JSON)
    tool_result     TEXT,                          -- text result returned by the tool
    token_count     INTEGER,                       -- optional token usage for this message
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT assert_column_type('public', 'agent_messages', 'message_id',     'int8');
SELECT assert_column_type('public', 'agent_messages', 'session_id',     'uuid');
SELECT assert_column_type('public', 'agent_messages', 'message_index',  'int4');
SELECT assert_column_type('public', 'agent_messages', 'role',           'text');
SELECT assert_column_type('public', 'agent_messages', 'content',        'text');
SELECT assert_column_type('public', 'agent_messages', 'tool_call_id',   'text');
SELECT assert_column_type('public', 'agent_messages', 'tool_name',      'text');
SELECT assert_column_type('public', 'agent_messages', 'tool_call_args', 'jsonb');
SELECT assert_column_type('public', 'agent_messages', 'tool_result',    'text');
SELECT assert_column_type('public', 'agent_messages', 'token_count',    'int4');
SELECT assert_column_type('public', 'agent_messages', 'created_at',     'timestamptz');

CREATE INDEX IF NOT EXISTS idx_agent_messages_session ON agent_messages(session_id, message_index);

-- ----------------------------------------------------------
-- 3. agent_memories
--    Long-term cognitive memory for agents: findings, plans,
--    insights, etc.  These persist across sessions so agents
--    can build up institutional knowledge about a binary.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_memories (
    memory_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      UUID REFERENCES agent_sessions(session_id)
                        ON DELETE SET NULL ON UPDATE CASCADE,
    binary_id       INTEGER REFERENCES binaries(id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    memory_type     TEXT NOT NULL DEFAULT 'finding' -- finding | plan | insight | error | custom
                        CHECK (memory_type IN ('finding','plan','insight','error','custom')),
    scope           TEXT NOT NULL DEFAULT 'session' -- session | binary | global
                        CHECK (scope IN ('session','binary','global')),
    key             TEXT,                          -- optional structured key for lookup
    content         TEXT NOT NULL,
    importance      REAL DEFAULT 0.5,              -- 0.0 – 1.0 relevance score
    metadata        JSONB,                         -- arbitrary extra data
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT assert_column_type('public', 'agent_memories', 'memory_id',   'int8');
SELECT assert_column_type('public', 'agent_memories', 'session_id',  'uuid');
SELECT assert_column_type('public', 'agent_memories', 'binary_id',  'int4');
SELECT assert_column_type('public', 'agent_memories', 'memory_type', 'text');
SELECT assert_column_type('public', 'agent_memories', 'scope',       'text');
SELECT assert_column_type('public', 'agent_memories', 'key',         'text');
SELECT assert_column_type('public', 'agent_memories', 'content',     'text');
SELECT assert_column_type('public', 'agent_memories', 'importance',  'float4');
SELECT assert_column_type('public', 'agent_memories', 'metadata',    'jsonb');
SELECT assert_column_type('public', 'agent_memories', 'created_at',  'timestamptz');

CREATE INDEX IF NOT EXISTS idx_agent_memories_session    ON agent_memories(session_id);
CREATE INDEX IF NOT EXISTS idx_agent_memories_binary     ON agent_memories(binary_id);
CREATE INDEX IF NOT EXISTS idx_agent_memories_type_scope ON agent_memories(memory_type, scope);

-- ----------------------------------------------------------
-- 4. agent_tool_calls
--    Audit log of every tool invocation: which tool, what
--    arguments, how long it took, and whether it succeeded.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_tool_calls (
    call_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      UUID NOT NULL REFERENCES agent_sessions(session_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    message_id      BIGINT REFERENCES agent_messages(message_id)
                        ON DELETE SET NULL ON UPDATE CASCADE,
    node_id         TEXT,                          -- which graph node issued this call
    tool_name       TEXT NOT NULL,
    tool_args       JSONB,                         -- arguments as JSON
    result_summary  TEXT,                          -- truncated result for audit
    success         BOOLEAN NOT NULL DEFAULT true,
    duration_ms     BIGINT,                        -- wall-clock duration in milliseconds
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT assert_column_type('public', 'agent_tool_calls', 'call_id',        'int8');
SELECT assert_column_type('public', 'agent_tool_calls', 'session_id',     'uuid');
SELECT assert_column_type('public', 'agent_tool_calls', 'message_id',     'int8');
SELECT assert_column_type('public', 'agent_tool_calls', 'node_id',        'text');
SELECT assert_column_type('public', 'agent_tool_calls', 'tool_name',      'text');
SELECT assert_column_type('public', 'agent_tool_calls', 'tool_args',      'jsonb');
SELECT assert_column_type('public', 'agent_tool_calls', 'result_summary', 'text');
SELECT assert_column_type('public', 'agent_tool_calls', 'success',        'bool');
SELECT assert_column_type('public', 'agent_tool_calls', 'duration_ms',    'int8');
SELECT assert_column_type('public', 'agent_tool_calls', 'created_at',     'timestamptz');

CREATE INDEX IF NOT EXISTS idx_agent_tool_calls_session ON agent_tool_calls(session_id);

-- ----------------------------------------------------------
-- 5. agent_session_contexts
--    Four-layer session state:
--      • environment — binary_id, project_path, program_name, etc.
--      • context     — config, ModuleData references
--      • cognitive   — cross-session memories / plans references
--      • conversation— message pointer (handled by agent_messages)
--    Only environment and context are stored here; cognitive and
--    conversation are stored in agent_memories / agent_messages.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_session_contexts (
    session_id      UUID PRIMARY KEY REFERENCES agent_sessions(session_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    environment     JSONB NOT NULL DEFAULT '{}',   -- binary_id, project_path, program_name, etc.
    context_config  JSONB NOT NULL DEFAULT '{}',   -- module config, model parameters, etc.
    module_data     JSONB NOT NULL DEFAULT '{}',   -- serialized ModuleData references
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT assert_column_type('public', 'agent_session_contexts', 'session_id',     'uuid');
SELECT assert_column_type('public', 'agent_session_contexts', 'environment',    'jsonb');
SELECT assert_column_type('public', 'agent_session_contexts', 'context_config', 'jsonb');
SELECT assert_column_type('public', 'agent_session_contexts', 'module_data',    'jsonb');
SELECT assert_column_type('public', 'agent_session_contexts', 'updated_at',     'timestamptz');

-- ----------------------------------------------------------
-- 6. agent_graphs
--    Defines a directed graph topology for multi-agent
--    collaboration.  A graph is bound to a session and
--    describes which agents exist and how they are connected.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_graphs (
    graph_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      UUID NOT NULL REFERENCES agent_sessions(session_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    graph_name      TEXT,                          -- optional label
    entry_node      TEXT,                          -- node_id of the entry point
    max_iterations  INTEGER DEFAULT 10,            -- safety limit for cycle resolution
    convergence_threshold REAL DEFAULT 0.95,       -- for CONVERGENCE cycle strategy
    cycle_strategy  TEXT NOT NULL DEFAULT 'MAX_ITERATIONS'
                        CHECK (cycle_strategy IN ('MAX_ITERATIONS','CONVERGENCE','MANUAL')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT assert_column_type('public', 'agent_graphs', 'graph_id',             'uuid');
SELECT assert_column_type('public', 'agent_graphs', 'session_id',           'uuid');
SELECT assert_column_type('public', 'agent_graphs', 'graph_name',           'text');
SELECT assert_column_type('public', 'agent_graphs', 'entry_node',           'text');
SELECT assert_column_type('public', 'agent_graphs', 'max_iterations',       'int4');
SELECT assert_column_type('public', 'agent_graphs', 'convergence_threshold','float4');
SELECT assert_column_type('public', 'agent_graphs', 'cycle_strategy',       'text');
SELECT assert_column_type('public', 'agent_graphs', 'created_at',           'timestamptz');

CREATE INDEX IF NOT EXISTS idx_agent_graphs_session ON agent_graphs(session_id);

-- ----------------------------------------------------------
-- 7. agent_graph_nodes
--    Each node represents an agent within a graph.  The
--    node's `role` determines how the graph engine routes
--    messages to/from it.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_graph_nodes (
    graph_id        UUID NOT NULL REFERENCES agent_graphs(graph_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    node_id         TEXT NOT NULL,                  -- unique within the graph
    node_name       TEXT,                           -- optional human-readable label
    role            TEXT NOT NULL DEFAULT 'WORKER'  -- SUPERVISOR | WORKER | CRITIC | SYNTHESIZER | GATEKEEPER | CUSTOM
                        CHECK (role IN ('SUPERVISOR','WORKER','CRITIC','SYNTHESIZER','GATEKEEPER','CUSTOM')),
    model_name      TEXT,                           -- overrides session-level model
    system_prompt   TEXT,                           -- system prompt for this agent
    tools           JSONB NOT NULL DEFAULT '[]',    -- list of tool names available
    config          JSONB NOT NULL DEFAULT '{}',    -- extra node-specific config
    PRIMARY KEY (graph_id, node_id)
);

SELECT assert_column_type('public', 'agent_graph_nodes', 'graph_id',      'uuid');
SELECT assert_column_type('public', 'agent_graph_nodes', 'node_id',       'text');
SELECT assert_column_type('public', 'agent_graph_nodes', 'node_name',     'text');
SELECT assert_column_type('public', 'agent_graph_nodes', 'role',          'text');
SELECT assert_column_type('public', 'agent_graph_nodes', 'model_name',    'text');
SELECT assert_column_type('public', 'agent_graph_nodes', 'system_prompt', 'text');
SELECT assert_column_type('public', 'agent_graph_nodes', 'tools',         'jsonb');
SELECT assert_column_type('public', 'agent_graph_nodes', 'config',        'jsonb');

-- ----------------------------------------------------------
-- 8. agent_graph_edges
--    Directed edges connecting graph nodes.  Edge type
--    determines the semantics of the connection.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_graph_edges (
    graph_id        UUID NOT NULL REFERENCES agent_graphs(graph_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    edge_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    source_node     TEXT NOT NULL,                  -- FK implied by (graph_id, node_id)
    target_node     TEXT NOT NULL,
    edge_type       TEXT NOT NULL DEFAULT 'DELEGATE' -- DELEGATE | REPORT | COLLABORATE | CRITIQUE | FEEDBACK | BROADCAST | GATE
                        CHECK (edge_type IN ('DELEGATE','REPORT','COLLABORATE','CRITIQUE','FEEDBACK','BROADCAST','GATE')),
    condition       TEXT,                           -- optional guard expression
    priority        INTEGER DEFAULT 0,              -- routing priority (higher = first)
    config          JSONB NOT NULL DEFAULT '{}',    -- edge-specific config
    PRIMARY KEY (graph_id, edge_id),
    FOREIGN KEY (graph_id, source_node) REFERENCES agent_graph_nodes(graph_id, node_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (graph_id, target_node) REFERENCES agent_graph_nodes(graph_id, node_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

SELECT assert_column_type('public', 'agent_graph_edges', 'graph_id',     'uuid');
SELECT assert_column_type('public', 'agent_graph_edges', 'edge_id',      'int8');
SELECT assert_column_type('public', 'agent_graph_edges', 'source_node',  'text');
SELECT assert_column_type('public', 'agent_graph_edges', 'target_node',  'text');
SELECT assert_column_type('public', 'agent_graph_edges', 'edge_type',    'text');
SELECT assert_column_type('public', 'agent_graph_edges', 'condition',    'text');
SELECT assert_column_type('public', 'agent_graph_edges', 'priority',     'int4');
SELECT assert_column_type('public', 'agent_graph_edges', 'config',       'jsonb');

CREATE INDEX IF NOT EXISTS idx_agent_graph_edges_graph ON agent_graph_edges(graph_id);

-- ----------------------------------------------------------
-- 9. agent_graph_executions
--    Runtime execution log for graph runs.  Each row records
--    one execution cycle (one node activation), enabling
--    step-by-step replay and debugging.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_graph_executions (
    execution_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    graph_id        UUID NOT NULL REFERENCES agent_graphs(graph_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    session_id      UUID NOT NULL REFERENCES agent_sessions(session_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    iteration       INTEGER NOT NULL,               -- which cycle/iteration this is
    node_id         TEXT NOT NULL,                   -- which node was activated
    input_summary   TEXT,                            -- truncated input sent to the node
    output_summary  TEXT,                            -- truncated output from the node
    status          TEXT NOT NULL DEFAULT 'running'  -- running | completed | failed | skipped
                        CHECK (status IN ('running','completed','failed','skipped')),
    duration_ms     BIGINT,                          -- wall-clock duration
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT assert_column_type('public', 'agent_graph_executions', 'execution_id',   'int8');
SELECT assert_column_type('public', 'agent_graph_executions', 'graph_id',       'uuid');
SELECT assert_column_type('public', 'agent_graph_executions', 'session_id',     'uuid');
SELECT assert_column_type('public', 'agent_graph_executions', 'iteration',      'int4');
SELECT assert_column_type('public', 'agent_graph_executions', 'node_id',        'text');
SELECT assert_column_type('public', 'agent_graph_executions', 'input_summary',  'text');
SELECT assert_column_type('public', 'agent_graph_executions', 'output_summary', 'text');
SELECT assert_column_type('public', 'agent_graph_executions', 'status',         'text');
SELECT assert_column_type('public', 'agent_graph_executions', 'duration_ms',    'int8');
SELECT assert_column_type('public', 'agent_graph_executions', 'created_at',     'timestamptz');

CREATE INDEX IF NOT EXISTS idx_agent_graph_executions_session    ON agent_graph_executions(session_id);
CREATE INDEX IF NOT EXISTS idx_agent_graph_executions_graph_iter ON agent_graph_executions(graph_id, iteration);

-- ----------------------------------------------------------
-- 10. agent_human_inputs
--     Pending human-in-the-loop input requests.  When an agent
--     needs human input, a row is inserted here.  The REST API
--     can poll this table to show pending requests, and a
--     subsequent API call updates the row with the human's
--     response.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_human_inputs (
    input_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      UUID NOT NULL REFERENCES agent_sessions(session_id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
    request_text    TEXT NOT NULL,                   -- the question/prompt shown to the human
    response_text   TEXT,                            -- the human's response (NULL until answered)
    status          TEXT NOT NULL DEFAULT 'pending'  -- pending | answered | cancelled | expired
                        CHECK (status IN ('pending','answered','cancelled','expired')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    answered_at     TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ                     -- optional deadline
);

SELECT assert_column_type('public', 'agent_human_inputs', 'input_id',      'int8');
SELECT assert_column_type('public', 'agent_human_inputs', 'session_id',    'uuid');
SELECT assert_column_type('public', 'agent_human_inputs', 'request_text',  'text');
SELECT assert_column_type('public', 'agent_human_inputs', 'response_text', 'text');
SELECT assert_column_type('public', 'agent_human_inputs', 'status',        'text');
SELECT assert_column_type('public', 'agent_human_inputs', 'created_at',    'timestamptz');
SELECT assert_column_type('public', 'agent_human_inputs', 'answered_at',   'timestamptz');
SELECT assert_column_type('public', 'agent_human_inputs', 'expires_at',    'timestamptz');

CREATE INDEX IF NOT EXISTS idx_agent_human_inputs_session ON agent_human_inputs(session_id);
CREATE INDEX IF NOT EXISTS idx_agent_human_inputs_status  ON agent_human_inputs(status);

-- ----------------------------------------------------------
-- 11. scripts
--     Persistent script storage within a database instance.
--     Each instance tracks its own scripts independently.
--     Note: no user_id FK here — per-instance DBs have no users
--     table; user ownership is tracked at the server level.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS scripts (
    id               SERIAL PRIMARY KEY,
    name             VARCHAR(255) NOT NULL,
    description      TEXT DEFAULT '',
    author           VARCHAR(255) DEFAULT '',
    code             TEXT NOT NULL,
    code_size        INTEGER DEFAULT 0,
    language         VARCHAR(20) DEFAULT 'kotlin',
    output           TEXT,
    output_size      INTEGER DEFAULT 0,
    status           VARCHAR(50) DEFAULT 'pending',
    save_result      BOOLEAN DEFAULT true,
    max_output_size  BIGINT DEFAULT 10485760,
    created_at       TIMESTAMPTZ DEFAULT now(),
    finished_at      TIMESTAMPTZ
);

SELECT assert_column_type('public', 'scripts', 'id',              'int4');
SELECT assert_column_type('public', 'scripts', 'name',            'varchar');
SELECT assert_column_type('public', 'scripts', 'description',     'text');
SELECT assert_column_type('public', 'scripts', 'author',          'varchar');
SELECT assert_column_type('public', 'scripts', 'code',            'text');
SELECT assert_column_type('public', 'scripts', 'code_size',       'int4');
SELECT assert_column_type('public', 'scripts', 'language',        'varchar');
SELECT assert_column_type('public', 'scripts', 'output',          'text');
SELECT assert_column_type('public', 'scripts', 'output_size',     'int4');
SELECT assert_column_type('public', 'scripts', 'status',          'varchar');
SELECT assert_column_type('public', 'scripts', 'save_result',     'bool');
SELECT assert_column_type('public', 'scripts', 'max_output_size', 'int8');
SELECT assert_column_type('public', 'scripts', 'created_at',      'timestamptz');
SELECT assert_column_type('public', 'scripts', 'finished_at',     'timestamptz');

-- ----------------------------------------------------------
-- 12. script_executions
--     Records of individual script runs.  Each execution may
--     target a specific binary_id.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS script_executions (
    id            SERIAL PRIMARY KEY,
    script_id     INTEGER REFERENCES scripts(id) ON DELETE CASCADE,
    binary_id     INTEGER,
    status        VARCHAR(50) DEFAULT 'pending',
    output        TEXT,
    error_message TEXT,
    started_at    TIMESTAMPTZ DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

SELECT assert_column_type('public', 'script_executions', 'id',            'int4');
SELECT assert_column_type('public', 'script_executions', 'script_id',     'int4');
SELECT assert_column_type('public', 'script_executions', 'binary_id',     'int4');
SELECT assert_column_type('public', 'script_executions', 'status',        'varchar');
SELECT assert_column_type('public', 'script_executions', 'output',        'text');
SELECT assert_column_type('public', 'script_executions', 'error_message', 'text');
SELECT assert_column_type('public', 'script_executions', 'started_at',    'timestamptz');
SELECT assert_column_type('public', 'script_executions', 'finished_at',   'timestamptz');
