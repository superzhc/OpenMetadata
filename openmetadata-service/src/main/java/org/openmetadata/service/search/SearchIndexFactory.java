package org.openmetadata.service.search;

import lombok.extern.slf4j.Slf4j;
import org.openmetadata.schema.analytics.ReportData;
import org.openmetadata.schema.entity.classification.Classification;
import org.openmetadata.schema.entity.classification.Tag;
import org.openmetadata.schema.entity.data.Chart;
import org.openmetadata.schema.entity.data.Container;
import org.openmetadata.schema.entity.data.Dashboard;
import org.openmetadata.schema.entity.data.DashboardDataModel;
import org.openmetadata.schema.entity.data.Database;
import org.openmetadata.schema.entity.data.DatabaseSchema;
import org.openmetadata.schema.entity.data.GlossaryTerm;
import org.openmetadata.schema.entity.data.MlModel;
import org.openmetadata.schema.entity.data.Pipeline;
import org.openmetadata.schema.entity.data.Query;
import org.openmetadata.schema.entity.data.StoredProcedure;
import org.openmetadata.schema.entity.data.Table;
import org.openmetadata.schema.entity.data.Topic;
import org.openmetadata.schema.entity.domains.DataProduct;
import org.openmetadata.schema.entity.domains.Domain;
import org.openmetadata.schema.entity.services.*;
import org.openmetadata.schema.entity.teams.Team;
import org.openmetadata.schema.entity.teams.User;
import org.openmetadata.schema.tests.TestCase;
import org.openmetadata.schema.tests.TestSuite;
import org.openmetadata.service.Entity;
import org.openmetadata.service.search.indexes.*;

@Slf4j
public class SearchIndexFactory {
  private SearchIndexFactory() {}

  public static SearchIndex buildIndex(String entityType, Object entity) {
    switch (entityType) {
      case Entity.TABLE:
        return new TableIndex((Table) entity);
      case Entity.DASHBOARD:
        return new DashboardIndex((Dashboard) entity);
      case Entity.TOPIC:
        return new TopicIndex((Topic) entity);
      case Entity.PIPELINE:
        return new PipelineIndex((Pipeline) entity);
      case Entity.USER:
        return new UserIndex((User) entity);
      case Entity.TEAM:
        return new TeamIndex((Team) entity);
      case Entity.GLOSSARY_TERM:
        return new GlossaryTermIndex((GlossaryTerm) entity);
      case Entity.MLMODEL:
        return new MlModelIndex((MlModel) entity);
      case Entity.TAG:
        return new TagIndex((Tag) entity);
      case Entity.CLASSIFICATION:
        return new ClassificationIndex((Classification) entity);
      case Entity.QUERY:
        return new QueryIndex((Query) entity);
      case Entity.CONTAINER:
        return new ContainerIndex((Container) entity);
      case Entity.DATABASE:
        return new DatabaseIndex((Database) entity);
      case Entity.DATABASE_SCHEMA:
        return new DatabaseSchemaIndex((DatabaseSchema) entity);
      case Entity.TEST_CASE:
        return new TestCaseIndex((TestCase) entity);
      case Entity.TEST_SUITE:
        return new TestSuiteIndex((TestSuite) entity);
      case Entity.CHART:
        return new ChartIndex((Chart) entity);
      case Entity.DASHBOARD_DATA_MODEL:
        return new DashboardDataModelIndex((DashboardDataModel) entity);
      case Entity.DASHBOARD_SERVICE:
        return new DashboardServiceIndex((DashboardService) entity);
      case Entity.DATABASE_SERVICE:
        return new DatabaseServiceIndex((DatabaseService) entity);
      case Entity.MESSAGING_SERVICE:
        return new MessagingServiceIndex((MessagingService) entity);
      case Entity.MLMODEL_SERVICE:
        return new MlModelServiceIndex((MlModelService) entity);
      case Entity.SEARCH_SERVICE:
        return new SearchServiceIndex((SearchService) entity);
      case Entity.SEARCH_INDEX:
        return new SearchEntityIndex((org.openmetadata.schema.entity.data.SearchIndex) entity);
      case Entity.PIPELINE_SERVICE:
        return new PipelineServiceIndex((PipelineService) entity);
      case Entity.STORAGE_SERVICE:
        return new StorageServiceIndex((StorageService) entity);
      case Entity.DOMAIN:
        return new DomainIndex((Domain) entity);
      case Entity.STORED_PROCEDURE:
        return new StoredProcedureIndex((StoredProcedure) entity);
      case Entity.DATA_PRODUCT:
        return new DataProductIndex((DataProduct) entity);
      case Entity.METADATA_SERVICE:
        return new MetadataServiceIndex((MetadataService) entity);
      case Entity.NETWORK_SERVICE:
        return new NetworkServiceIndex((NetworkService) entity);
      case Entity.ENTITY_REPORT_DATA:
        return new EntityReportDataIndex((ReportData) entity);
      case Entity.WEB_ANALYTIC_ENTITY_VIEW_REPORT_DATA:
        return new WebAnalyticEntityViewReportDataIndex((ReportData) entity);
      case Entity.WEB_ANALYTIC_USER_ACTIVITY_REPORT_DATA:
        return new WebAnalyticUserActivityReportDataIndex((ReportData) entity);
      case Entity.RAW_COST_ANALYSIS_REPORT_DATA:
        return new RawCostAnalysisReportDataIndex((ReportData) entity);
      case Entity.AGGREGATED_COST_ANALYSIS_REPORT_DATA:
        return new AggregatedCostAnalysisReportDataIndex((ReportData) entity);
      default:
        LOG.warn("Ignoring Entity Type {}", entityType);
    }
    throw new IllegalArgumentException(String.format("Entity Type [%s] is not valid for Index Factory", entityType));
  }
}
