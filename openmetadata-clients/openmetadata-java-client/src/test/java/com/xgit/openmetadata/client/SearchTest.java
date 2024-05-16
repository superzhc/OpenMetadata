package com.xgit.openmetadata.client;

import com.xgit.openmetadata.client.config.ClientConfig;
import com.xgit.openmetadata.client.config.DevServerConfig;
import feign.Response;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.ElasticSearchApi;
import org.openmetadata.client.api.SearchApi;
import org.openmetadata.client.model.SearchResponse;

public class SearchTest extends OpenMetadataTest {
  SearchApi api;
  ElasticSearchApi newApi;

  @Before
  public void setUp() throws Exception {
    api = apiClient().buildClient(SearchApi.class);
    newApi = apiClient().buildClient(ElasticSearchApi.class);
  }

  @Override
  protected ClientConfig initClientConfig() {
    return new DevServerConfig();
  }

  /** 使用该接口报错，官网issue：https://github.com/open-metadata/OpenMetadata/issues/11990 */
  @Test
  public void testQuery() {
    SearchApi.SearchEntitiesWithQueryQueryParams params = new SearchApi.SearchEntitiesWithQueryQueryParams();

    SearchResponse response =
        api.searchEntitiesWithQuery(
            "test",
            "table_search_index",
            false,
            0,
            10,
            "_score",
            "desc",
            false,
            "{\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"term\":{\"tags.tagFQN\":\"是否共享.Share\"}}]}},{\"bool\":{\"should\":[{\"term\":{\"service.name.keyword\":\"dtc_dw_clickhouse\"}}]}}]}}}",
            null,
            true,
            null);
    System.out.println(response);
  }

  @Test
  public void testNewQuery() throws IOException {
    ElasticSearchApi.SearchEntitiesWithQueryQueryParams params =
        new ElasticSearchApi.SearchEntitiesWithQueryQueryParams();
    params
        .q("test")
        .index("table_search_index")
        .deleted(false)
        .from(0)
        .size(10)
        .queryFilter(
            "{\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"term\":{\"tags.tagFQN\":\"是否共享.Share\"}}]}},{\"bool\":{\"should\":[{\"term\":{\"service.name.keyword\":\"dtc_dw_clickhouse\"}}]}}]}}}")
        .fetchSource(true)
    // .includeSourceFields(null)
    ;
    Response response = newApi.searchEntitiesWithQuery(params);
    System.out.println(IOUtils.toString(response.body().asInputStream()));
  }
}
