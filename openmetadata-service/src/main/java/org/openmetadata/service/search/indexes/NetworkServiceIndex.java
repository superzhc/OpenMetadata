package org.openmetadata.service.search.indexes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.openmetadata.schema.entity.services.NetworkService;
import org.openmetadata.service.Entity;
import org.openmetadata.service.search.SearchIndexUtils;
import org.openmetadata.service.search.models.SearchSuggest;
import org.openmetadata.service.util.JsonUtils;

public class NetworkServiceIndex implements SearchIndex {

  final NetworkService networkService;

  private static final List<String> excludeFields = List.of("changeDescription");

  public NetworkServiceIndex(NetworkService networkService) {
    this.networkService = networkService;
  }

  public Map<String, Object> buildESDoc() {
    Map<String, Object> doc = JsonUtils.getMap(networkService);
    SearchIndexUtils.removeNonIndexableFields(doc, excludeFields);
    List<SearchSuggest> suggest = new ArrayList<>();
    suggest.add(SearchSuggest.builder().input(networkService.getName()).weight(5).build());
    suggest.add(SearchSuggest.builder().input(networkService.getFullyQualifiedName()).weight(5).build());
    doc.put("suggest", suggest);
    doc.put("entityType", Entity.PIPELINE_SERVICE);
    doc.put(
        "fqnParts",
        getFQNParts(
            networkService.getFullyQualifiedName(),
            suggest.stream().map(SearchSuggest::getInput).collect(Collectors.toList())));
    if (networkService.getOwner() != null) {
      doc.put("owner", getOwnerWithDisplayName(networkService.getOwner()));
    }
    return doc;
  }
}
