package org.openmetadata.service.jdbi3;

import org.openmetadata.schema.entity.services.NetworkService;
import org.openmetadata.schema.entity.services.ServiceType;
import org.openmetadata.schema.type.NetworkConnection;
import org.openmetadata.service.Entity;
import org.openmetadata.service.resources.services.network.NetworkServiceResource;

public class NetworkServiceRepository extends ServiceEntityRepository<NetworkService, NetworkConnection> {
  private static final String UPDATE_FIELDS = "owner,connection";

  public NetworkServiceRepository() {
    super(
        NetworkServiceResource.COLLECTION_PATH,
        Entity.NETWORK_SERVICE,
        Entity.getCollectionDAO().networkServiceDAO(),
        NetworkConnection.class,
        UPDATE_FIELDS,
        ServiceType.NETWORK);
    // 2024年9月3日 网络服务支持索引搜索
    supportsSearch = true;
  }
}
