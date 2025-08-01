UPDATE dashboard_data_model_entity
SET json = JSON_MERGE_PATCH(
    json,
    '{"dataModelType":"TableauDataModel"}'
)
WHERE JSON_QUERY(json, '$.dataModelType') = '"TableauSheet"';