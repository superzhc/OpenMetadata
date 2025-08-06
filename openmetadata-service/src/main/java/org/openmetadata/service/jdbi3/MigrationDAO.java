package org.openmetadata.service.jdbi3;

import static org.openmetadata.service.jdbi3.locator.ConnectionType.*;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.jdbi.v3.core.statement.StatementException;
import org.jdbi.v3.sqlobject.SingleValue;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.openmetadata.service.jdbi3.locator.ConnectionAwareSqlQuery;
import org.openmetadata.service.jdbi3.locator.ConnectionAwareSqlUpdate;

public interface MigrationDAO {
  @ConnectionAwareSqlQuery(value = "SELECT MAX(version) FROM DATABASE_CHANGE_LOG", connectionType = MYSQL)
  @ConnectionAwareSqlQuery(value = "SELECT max(version) FROM \"DATABASE_CHANGE_LOG\"", connectionType = POSTGRES)
  @ConnectionAwareSqlQuery(value = "SELECT MAX(version) FROM DATABASE_CHANGE_LOG", connectionType = DAMENG)
  @SingleValue
  Optional<String> getMaxVersion() throws StatementException;

  @ConnectionAwareSqlQuery(value = "SELECT MAX(version) FROM SERVER_CHANGE_LOG", connectionType = MYSQL)
  @ConnectionAwareSqlQuery(value = "SELECT max(version) FROM SERVER_CHANGE_LOG", connectionType = POSTGRES)
  @ConnectionAwareSqlQuery(value = "SELECT MAX(version) FROM SERVER_CHANGE_LOG", connectionType = DAMENG)
  @SingleValue
  Optional<String> getMaxServerMigrationVersion() throws StatementException;

  @ConnectionAwareSqlQuery(
      value = "SELECT checksum FROM SERVER_CHANGE_LOG where version = :version",
      connectionType = MYSQL)
  @ConnectionAwareSqlQuery(
      value = "SELECT checksum FROM SERVER_CHANGE_LOG where version = :version",
      connectionType = POSTGRES)
  @ConnectionAwareSqlQuery(
      value = "SELECT checksum FROM SERVER_CHANGE_LOG where version = :version",
      connectionType = DAMENG)
  String getVersionMigrationChecksum(@Bind("version") String version) throws StatementException;

  @ConnectionAwareSqlQuery(
      value = "SELECT sqlStatement FROM SERVER_MIGRATION_SQL_LOGS where version = :version and checksum = :checksum",
      connectionType = MYSQL)
  @ConnectionAwareSqlQuery(
      value = "SELECT sqlStatement FROM SERVER_MIGRATION_SQL_LOGS where version = :version and checksum = :checksum",
      connectionType = POSTGRES)
  @ConnectionAwareSqlQuery(
      value = "SELECT sqlStatement FROM SERVER_MIGRATION_SQL_LOGS where version = :version and checksum = :checksum",
      connectionType = DAMENG)
  String getSqlQuery(@Bind("version") String version, @Bind("checksum") String checksum) throws StatementException;

  @ConnectionAwareSqlUpdate(
      value =
          "INSERT INTO SERVER_CHANGE_LOG (version, migrationFileName, checksum, installed_on)"
              + "VALUES (:version, :migrationFileName, :checksum, CURRENT_TIMESTAMP) "
              + "ON DUPLICATE KEY UPDATE "
              + "migrationFileName = :migrationFileName, "
              + "checksum = :checksum, "
              + "installed_on = CURRENT_TIMESTAMP",
      connectionType = MYSQL)
  @ConnectionAwareSqlUpdate(
      value =
          "INSERT INTO server_change_log (version, migrationFileName, checksum, installed_on)"
              + "VALUES (:version, :migrationFileName, :checksum, current_timestamp) "
              + "ON CONFLICT (version) DO UPDATE SET "
              + "migrationFileName = EXCLUDED.migrationFileName, "
              + "checksum = EXCLUDED.checksum, "
              + "installed_on = EXCLUDED.installed_on",
      connectionType = POSTGRES)
  @ConnectionAwareSqlUpdate(
      value =
          "MERGE INTO SERVER_CHANGE_LOG AS t1 "
              + "USING (SELECT :version AS version, :migrationFileName AS migrationFileName, :checksum AS checksum, CURRENT_TIMESTAMP AS installed_on FROM dual) AS t2 "
              + "ON(t1.version=t2.version) "
              + "WHEN MATCHED THEN UPDATE SET migrationFileName = t2.migrationFileName,checksum = t2.checksum,installed_on = t2.installed_on "
              + "WHEN NOT MATCHED THEN INSERT (version, migrationFileName, checksum, installed_on) VALUES (t2.version, t2.migrationFileName, t2.checksum, t2.installed_on)",
      connectionType = DAMENG)
  void upsertServerMigration(
      @Bind("version") String version,
      @Bind("migrationFileName") String migrationFileName,
      @Bind("checksum") String checksum);

  @ConnectionAwareSqlUpdate(
      value =
          "INSERT INTO SERVER_MIGRATION_SQL_LOGS (version, sqlStatement, checksum, executedAt)"
              + "VALUES (:version, :sqlStatement, :checksum, CURRENT_TIMESTAMP) "
              + "ON DUPLICATE KEY UPDATE "
              + "version = :version, "
              + "sqlStatement = :sqlStatement, "
              + "executedAt = CURRENT_TIMESTAMP",
      connectionType = MYSQL)
  @ConnectionAwareSqlUpdate(
      value =
          "INSERT INTO SERVER_MIGRATION_SQL_LOGS (version, sqlStatement, checksum, executedAt)"
              + "VALUES (:version, :sqlStatement, :checksum, current_timestamp) "
              + "ON CONFLICT (checksum) DO UPDATE SET "
              + "version = EXCLUDED.version, "
              + "sqlStatement = EXCLUDED.sqlStatement, "
              + "executedAt = EXCLUDED.executedAt",
      connectionType = POSTGRES)
  @ConnectionAwareSqlUpdate(
      value =
          "MERGE INTO SERVER_MIGRATION_SQL_LOGS AS t1 "
              + "USING (SELECT :version AS version, :sqlStatement AS sqlStatement, :checksum AS checksum, CURRENT_TIMESTAMP AS executedAt FROM dual) AS t2 "
              + "ON(t1.checksum=t2.checksum) "
              + "WHEN MATCHED THEN UPDATE SET version = t2.version,sqlStatement = t2.sqlStatement,executedAt = t2.executedAt "
              + "WHEN NOT MATCHED THEN INSERT (version, sqlStatement, checksum, executedAt) VALUES (t2.version, t2.sqlStatement, t2.checksum, t2.executedAt)",
      connectionType = DAMENG)
  void upsertServerMigrationSQL(
      @Bind("version") String version, @Bind("sqlStatement") String sqlStatement, @Bind("checksum") String success);

  @ConnectionAwareSqlQuery(
      value = "SELECT checksum FROM SERVER_MIGRATION_SQL_LOGS where version = :version",
      connectionType = MYSQL)
  @ConnectionAwareSqlQuery(
      value = "SELECT checksum FROM SERVER_MIGRATION_SQL_LOGS where version = :version",
      connectionType = POSTGRES)
  @ConnectionAwareSqlQuery(
      value = "SELECT checksum FROM SERVER_MIGRATION_SQL_LOGS where version = :version",
      connectionType = DAMENG)
  List<String> getServerMigrationSQLWithVersion(@Bind("version") String version);

  @ConnectionAwareSqlQuery(
      value = "SELECT sqlStatement FROM SERVER_MIGRATION_SQL_LOGS where checksum = :checksum",
      connectionType = MYSQL)
  @ConnectionAwareSqlQuery(
      value = "SELECT sqlStatement FROM SERVER_MIGRATION_SQL_LOGS where checksum = :checksum",
      connectionType = POSTGRES)
  @ConnectionAwareSqlQuery(
      value = "SELECT checksum FROM SERVER_MIGRATION_SQL_LOGS where checksum = :checksum",
      connectionType = DAMENG)
  String checkIfQueryPreviouslyRan(@Bind("checksum") String checksum);

  @Getter
  @Setter
  class ServerMigrationSQLTable {
    private String version;
    private String sqlStatement;
    private String checkSum;
  }
}
