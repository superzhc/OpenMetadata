package org.openmetadata.service.secrets.converter;

import java.util.List;
import org.openmetadata.schema.metadataIngestion.*;
import org.openmetadata.service.util.JsonUtils;

public class SourceConfigClassConverter extends ClassConverter {

  private static final List<Class<?>> CONFIG_SOURCE_CLASSES =
      List.of(
          DatabaseServiceMetadataPipeline.class,
          DatabaseServiceQueryUsagePipeline.class,
          DatabaseServiceQueryLineagePipeline.class,
          DashboardServiceMetadataPipeline.class,
          MessagingServiceMetadataPipeline.class,
          DatabaseServiceProfilerPipeline.class,
          PipelineServiceMetadataPipeline.class,
          MlmodelServiceMetadataPipeline.class,
          StorageServiceMetadataPipeline.class,
          SearchServiceMetadataPipeline.class,
          TestSuitePipeline.class,
          MetadataToElasticSearchPipeline.class,
          DataInsightPipeline.class,
          DbtPipeline.class,
          ApplicationPipeline.class);

  protected SourceConfigClassConverter() {
    super(SourceConfig.class);
  }

  @Override
  public Object convert(Object object) {
    SourceConfig sourceConfig = (SourceConfig) JsonUtils.convertValue(object, this.clazz);
    tryToConvertOrFail(sourceConfig.getConfig(), CONFIG_SOURCE_CLASSES).ifPresent(data -> sourceConfig.setConfig(data));
    return sourceConfig;
  }
}
