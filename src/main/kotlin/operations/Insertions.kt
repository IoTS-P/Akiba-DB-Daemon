package org.iotsplab.akiba.dbDaemon.operations

import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.header
import org.iotsplab.akiba.dbDaemon.AbstractPostRoute
import org.iotsplab.akiba.dbDaemon.RouteContext
import org.iotsplab.akiba.dbDaemon.dbutil.UserDatabaseSession
import org.iotsplab.akiba.dbDaemon.operations.PGInstances.tokens
import java.sql.SQLException
import java.util.UUID

object Insertions {
    const val DATABASE_INSERT_BINARY_COMMAND = """
        INSERT INTO binaries 
        (original_path, checksum, size, arch, format, compiler_spec, source_id, source_module,
         processed_path, processed_checksum, processed_size) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    // ============================================================
    // -------------------------- ROUTES --------------------------
    // ============================================================

    object MD5Exists : AbstractPostRoute() {
        override val path: String = "/insert/check_md5"

        override suspend fun handle(ctx: RouteContext): Any? {
            val md5 = ctx.receive<String>()
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            return fa.session.md5List.contains(md5)
        }
    }

    object InsertBinary : AbstractPostRoute() {
        data class InsertData(
            val originalPath: String,
            val processedPath: String? = null,
            val checksum: String,
            val processedChecksum: String? = null,
            val size: Long,
            val processedSize: Long = -1,
            // Null until the architecture-identification module settles them;
            // setString(pos, null) writes SQL NULL, which the column allows.
            val arch: String? = null,
            val format: String? = null,
            val compilerSpec: String? = null,
            // Provenance for files imported at runtime by a module:
            //   sourceId     = id of the binary being analyzed when this file was imported
            //                  (null for top-level imports done by ImportManager)
            //   sourceModule = simple class name of the importing AkibaModule
            //                  (null for top-level imports done by ImportManager)
            val sourceId: Int? = null,
            val sourceModule: String? = null,
        )

        override val path: String = "/insert/insert_bin"

        @Throws(SQLException::class)
        override suspend fun handle(ctx: RouteContext): Any? {
            val data = ctx.receive<InsertData>()
            val processed = data.processedPath != null || data.processedChecksum != null
            if (processed && (data.processedPath == null || data.processedChecksum == null))
                return HttpStatusCode.BadRequest.description("Invalid processed binary data")
            val fa = try {
                fastAccess(ctx)
            } catch (e: FastAccessException) {
                return e.code
            }

            fa.session.useDb { conn ->
                conn.prepareStatement(DATABASE_INSERT_BINARY_COMMAND).use {
                    it.setString(1, data.originalPath)
                    it.setString(2, data.checksum)
                    it.setLong(3, data.size)
                    it.setString(4, data.arch)
                    it.setString(5, data.format)
                    it.setString(6, data.compilerSpec)
                    if (data.sourceId != null)
                        it.setInt(7, data.sourceId)
                    else
                        it.setNull(7, java.sql.Types.INTEGER)
                    if (data.sourceModule != null)
                        it.setString(8, data.sourceModule)
                    else
                        it.setNull(8, java.sql.Types.VARCHAR)
                    it.setString(9, data.processedPath)
                    it.setString(10, data.processedChecksum)
                    if (data.processedSize >= 0)
                        it.setLong(11, data.processedSize)
                    else
                        it.setNull(11, java.sql.Types.BIGINT)
                    it.executeUpdate()
                }
            }
            fa.session.md5List.add(data.checksum)

            if (data.processedChecksum != null)
                fa.session.md5List.add(data.processedChecksum)

            return fa.session.getLastInsertId()
        }
    }
}