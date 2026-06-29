package org.iotsplab.akiba.dbDaemon

import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respond
import io.ktor.server.routing.*
import org.iotsplab.akiba.dbDaemon.operations.AgentOps
import org.iotsplab.akiba.dbDaemon.operations.ArtifactOps
import org.iotsplab.akiba.dbDaemon.operations.Backups
import org.iotsplab.akiba.dbDaemon.operations.Controls
import org.iotsplab.akiba.dbDaemon.operations.Insertions
import org.iotsplab.akiba.dbDaemon.operations.MailboxOps
import org.iotsplab.akiba.dbDaemon.operations.Modules
import org.iotsplab.akiba.dbDaemon.operations.PGInstances
import org.iotsplab.akiba.dbDaemon.operations.Queries
import org.iotsplab.akiba.dbDaemon.operations.ScriptOps

object RouteRegistry {

    // Add routes here
    private val postRoutes: List<AbstractPostRoute> = listOf(
        Queries.SelectIdPage,
        Queries.SelectIdCount,
        Queries.GetBinaryMetadata,
        Queries.SearchBinaries,
        Queries.GetModuleData,
        Queries.SelectIdInSQL,

        Insertions.MD5Exists,
        Insertions.InsertBinary,

        Modules.CreateModuleTable,
        Modules.CreateView,
        Modules.TableLock,
        Modules.TableUnlock,
        Modules.UpdateData,
        Modules.StartTask,
        Modules.FinishTask,

        Controls.EnableRoute,
        Controls.DisableRoute,
        Controls.Heartbeat,

        PGInstances.ClientLogin,
        PGInstances.ClientLogout,
        PGInstances.ConnectToInstance,
        PGInstances.DisconnectFromInstance,
        PGInstances.ShutdownInstance,
        PGInstances.DeleteInstance,

        Backups.PeekBackup,

        // Agent Framework routes
        AgentOps.CreateSession,
        AgentOps.GetSession,
        AgentOps.GetSessionChildren,
        AgentOps.ListSessions,
        AgentOps.UpdateSession,
        AgentOps.SetSessionLifecycle,
        AgentOps.SetRuntimeState,
        AgentOps.GetRuntimeState,
        AgentOps.ListLiveSubtree,
        AgentOps.GetAgentStatus,

        AgentOps.AppendMessages,
        AgentOps.GetMessages,
        AgentOps.DeleteMessagesFrom,

        AgentOps.StoreMemory,
        AgentOps.QueryMemories,

        AgentOps.SaveContext,
        AgentOps.LoadContext,

        AgentOps.CreateGraph,
        AgentOps.AddGraphNode,
        AgentOps.AddGraphEdge,
        AgentOps.LoadGraph,

        AgentOps.RecordExecution,
        AgentOps.GetExecutions,

        AgentOps.RequestHumanInput,
        AgentOps.RespondHumanInput,
        AgentOps.PollHumanInputs,

        AgentOps.RecordToolCall,
        AgentOps.FindToolCallResults,
        AgentOps.GetToolCallResult,
        AgentOps.AppendTranscript,

        // Mailbox routes
        MailboxOps.SendMessage,
        MailboxOps.ListMessages,
        MailboxOps.DrainMessages,
        MailboxOps.AckMessage,
        MailboxOps.CountUnread,
        MailboxOps.GetMessage,

        // Artifact routes
        ArtifactOps.PublishArtifact,
        ArtifactOps.GetArtifact,
        ArtifactOps.ListArtifacts,
        ArtifactOps.DeleteArtifact,

        // Script routes
        ScriptOps.CreateScript,
        ScriptOps.GetScript,
        ScriptOps.ListScripts,
        ScriptOps.UpdateScript,
        ScriptOps.UpdateScriptOutput,
        ScriptOps.DeleteScript,

        ScriptOps.CreateExecution,
        ScriptOps.GetExecution,
        ScriptOps.ListExecutions,
        ScriptOps.UpdateExecution,
        ScriptOps.DeleteOldExecutions
    )

    private val wsRoutes: List<AbstractWebsocketRoute> = listOf(
        PGInstances.CreateInstance,

        Backups.CreateBackup,
        Backups.RestoreBackup
    )

    // Register all routes
    fun registerAll(routing: Routing) {
        // register test route
        routing.get("/test") {
            call.respond("The server works well")
        }
        routing.post("/test") {
            call.respond("The server works well")
        }

        postRoutes.forEach { it.registerPost(routing) }
        wsRoutes.forEach { it.registerWebsocket(routing) }
    }

    fun enable(route: String): Any? {
        postRoutes.firstOrNull { it.path == route } ?.let {
            it.disabled = false
            return HttpStatusCode.OK
        } ?: run {
            return HttpStatusCode.NotFound
        }
    }

    fun disable(route: String): Any? {
        postRoutes.firstOrNull { it.path == route } ?.let {
            it.disabled = true
            return HttpStatusCode.OK
        } ?: run {
            return HttpStatusCode.NotFound
        }
    }
}