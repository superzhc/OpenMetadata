package org.openmetadata.service.jdbi3;

import lombok.extern.slf4j.Slf4j;
import org.openmetadata.schema.entity.data.UniversalDataModel;
import org.openmetadata.service.Entity;
import org.openmetadata.service.resources.universalmodels.UniversalDataModelResource;
import org.openmetadata.service.util.EntityUtil;

@Slf4j
public class UniversalDataModelRepository extends EntityRepository<UniversalDataModel> {
  public UniversalDataModelRepository() {
    super(
        UniversalDataModelResource.COLLECTION_PATH,
        Entity.UNIVERSAL_DATA_MODEL,
        UniversalDataModel.class,
        Entity.getCollectionDAO().universalDataModelDAO(),
        "",
        "");
    supportsSearch = false;
  }

  @Override
  public UniversalDataModel setFields(UniversalDataModel entity, EntityUtil.Fields fields) {
    return null;
  }

  @Override
  public UniversalDataModel clearFields(UniversalDataModel entity, EntityUtil.Fields fields) {
    return null;
  }

  @Override
  public void prepare(UniversalDataModel entity, boolean update) {}

  @Override
  public void storeEntity(UniversalDataModel entity, boolean update) {
    store(entity, update);
  }

  @Override
  public void storeRelationships(UniversalDataModel entity) {}
}
