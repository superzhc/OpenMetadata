package org.openmetadata.service.search.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.openmetadata.schema.EntityInterface;

/** 类似Elasticsearch的返回结果格式 */
public class DBSearchResponse {
  static class Hits {
    @Getter @Setter Total total;
    @Getter @Setter List<Hit> hits = new ArrayList<>();
  }

  static class Hit {
    @Getter @Setter String _id;
    @Getter @Setter String _index;
    @Getter @Setter Object _source;

    Hit() {}

    Hit(String _id, Object _source) {
      this._id = _id;
      this._source = _source;
    }
  }

  static class Total {
    @Getter @Setter Long value = 0L;
  }

  public static class Aggregation {
    @Getter @Setter Integer doc_count_error_upper_bound = 0;
    @Getter @Setter Integer sum_other_doc_count = 0;
    @Getter List<Bucket> buckets = new ArrayList<>();
  }

  public static class Bucket {
    @Getter @Setter String key;
    @Getter @Setter Integer doc_count;

    public Bucket(String key, Integer doc_count) {
      this.key = key;
      this.doc_count = doc_count;
    }
  }

  @Getter @Setter Integer took = 0;
  @Getter Hits hits = new Hits();
  @Getter Map<String, Aggregation> aggregations = new HashMap<>();

  public static DBSearchResponse emptyResponse() {
    return new DBSearchResponse();
  }

  public static DBSearchResponse maskResponse() {
    DBSearchResponse response = new DBSearchResponse();

    Aggregation emptyAggregation = new Aggregation();
    response.addAggregation("sterms#serviceType", emptyAggregation);
    response.addAggregation("sterms#tags.tagFQN", emptyAggregation);
    response.addAggregation("sterms#entityType.keyword", emptyAggregation);
    response.addAggregation("sterms#domain.displayName.keyword", emptyAggregation);
    response.addAggregation("sterms#tier.tagFQN", emptyAggregation);
    response.addAggregation("sterms#service.name.keyword", emptyAggregation);
    response.addAggregation("sterms#owner.displayName.keyword", emptyAggregation);

    return response;
  }

  public DBSearchResponse addHit(EntityInterface entity) {
    hits.getHits().add(new Hit(entity.getId().toString(), entity));
    return this;
  }

  public DBSearchResponse setTotal(Long total) {
    hits.getTotal().setValue(total);
    return this;
  }

  public DBSearchResponse addAggregation(String name, Aggregation result) {
    aggregations.put(name, result);
    return this;
  }
}
