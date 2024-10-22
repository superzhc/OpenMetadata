package org.openmetadata.service.search.database;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.openmetadata.schema.dataInsight.DataInsightChartResult;
import org.openmetadata.schema.service.configuration.elasticsearch.ElasticSearchConfiguration;
import org.openmetadata.service.Entity;
import org.openmetadata.service.jdbi3.CollectionDAO;
import org.openmetadata.service.search.SearchClient;
import org.openmetadata.service.search.SearchRequest;
import org.openmetadata.service.search.models.IndexMapping;

public class DBSearchClient implements SearchClient {

  private final CollectionDAO dao;
  private final CollectionDAO.SearchDAO searchDao;

  public DBSearchClient(ElasticSearchConfiguration config) {
    this.dao = Entity.getCollectionDAO();
    this.searchDao = this.dao.searchDAO();
  }

  @Override
  public boolean isClientAvailable() {
    return true;
  }

  @Override
  public ElasticSearchConfiguration.SearchType getSearchType() {
    return ElasticSearchConfiguration.SearchType.DB;
  }

  @Override
  public boolean indexExists(String indexName) {
    // 数据库搜索不存在表无需考虑对应的索引是否存在
    return true;
  }

  @Override
  public boolean createIndex(IndexMapping indexMapping, String indexMappingContent) {
    /*throw new DBSearchException("数据库搜索不支持创建索引");*/
    return true;
  }

  @Override
  public void updateIndex(IndexMapping indexMapping, String indexMappingContent) {
    /*throw new DBSearchException("数据库搜索不支持更新索引");*/
  }

  @Override
  public void deleteIndex(IndexMapping indexMapping) {
    /*throw new DBSearchException("数据库搜索不支持删除索引");*/
  }

  @Override
  public void createAliases(IndexMapping indexMapping) {
    /*throw new DBSearchException("数据库搜索不支持创建索引别名");*/
  }

  @Override
  public Response search(SearchRequest request) throws IOException {
    // 2024年10月18日 多索引使用逗号进行分割，结果需要合并
    switch (request.getIndex()) {
      case "database_service_search_index":
      case "database_search_index":
      case "database_schema_search_index":
      case "table_search_index":
        // 查询耗时较长
        //        EntityRepository repository= Entity.getEntityRepository(Entity.TABLE);
        //        Object data=repository.searchListWithOffset(repository.getFields(""),new ListFilter(),
        // request.getSize(), request.getFrom());
        //        return Response.status(Response.Status.OK).entity(data).build();
      case "messaging_service_search_index":
      case "topic_search_index":
      case "network_service_search_index":
      case "glossary_term_search_index":
      default:
        break;
    }
    return Response.status(Response.Status.OK).entity(null).build();
  }

  @Override
  public Response searchBySourceUrl(String sourceUrl) throws IOException {
    return null;
  }

  @Override
  public Response searchByField(String fieldName, String fieldValue, String index) throws IOException {
    return null;
  }

  @Override
  public Response aggregate(String index, String fieldName, String value, String query) throws IOException {
    return null;
  }

  @Override
  public Response suggest(SearchRequest request) throws IOException {
    return null;
  }

  @Override
  public void createEntity(String indexName, String docId, String doc) {
    /*throw new DBSearchException("数据库搜索不支持新增数据");*/
  }

  @Override
  public void createTimeSeriesEntity(String indexName, String docId, String doc) {
    /*throw new DBSearchException("数据库搜索不支持新增时序数据");*/
  }

  @Override
  public void updateEntity(String indexName, String docId, Map<String, Object> doc, String scriptTxt) {
    /*throw new DBSearchException("数据库搜索不支持更新数据");*/
  }

  @Override
  public void deleteByScript(String indexName, String scriptTxt, Map<String, Object> params) {
    /*throw new DBSearchException("数据库搜索不支持通过脚本删除数据");*/
  }

  @Override
  public void deleteEntity(String indexName, String docId) {
    /*throw new DBSearchException("数据库搜索不支持删除数据");*/
  }

  @Override
  public void deleteEntityByFields(String indexName, List<Pair<String, String>> fieldAndValue) {
    /*throw new DBSearchException("数据库搜索不支持通过字段删除数据");*/
  }

  @Override
  public void softDeleteOrRestoreEntity(String indexName, String docId, String scriptTxt) {
    /*throw new DBSearchException("数据库搜索不支持逻辑删除或恢复数据");*/
  }

  @Override
  public void softDeleteOrRestoreChildren(
      String indexName, String scriptTxt, List<Pair<String, String>> fieldAndValue) {
    /*throw new DBSearchException("数据库搜索不支持逻辑删除或恢复子数据");*/
  }

  @Override
  public void updateChildren(
      String indexName, Pair<String, String> fieldAndValue, Pair<String, Map<String, Object>> updates) {
    /*throw new DBSearchException("数据库搜索不支持更新子数据");*/
  }

  @Override
  public TreeMap<Long, List<Object>> getSortedDate(
      String team,
      Long scheduleTime,
      Long currentTime,
      DataInsightChartResult.DataInsightChartType chartType,
      String indexName)
      throws IOException, ParseException {
    return null;
  }

  @Override
  public Response listDataInsightChartResult(
      Long startTs,
      Long endTs,
      String tier,
      String team,
      DataInsightChartResult.DataInsightChartType dataInsightChartName,
      Integer size,
      Integer from,
      String queryFilter,
      String dataReportIndex)
      throws IOException, ParseException {
    return null;
  }

  @Override
  public void close() {
    // 注意：一定不要在这边关闭该dao，该dao从Entity.getCollectionDAO()中获取
    // ignore
  }
}
