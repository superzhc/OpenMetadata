
-- We'll rank all the runs (timestamps) for every day, and delete all the data but the most recent one.
DELETE FROM report_data_time_series
WHERE JSON_VALUE(json, '$.id') IN (
    SELECT ids FROM (
        SELECT
        JSON_VALUE(json, '$.id') AS ids,
        DENSE_RANK() OVER(PARTITION BY date ORDER BY timestamp DESC) as denseRank
        FROM (
            SELECT *
            FROM report_data_time_series rdts
            WHERE JSON_VALUE(json, '$.reportDataType') = 'WebAnalyticEntityViewReportData'
        ) duplicates
        ORDER BY date DESC, timestamp DESC
    ) dense_ranked
    WHERE denseRank != 1
);

DELETE FROM report_data_time_series
WHERE JSON_VALUE(json, '$.id') IN (
    SELECT ids FROM (
        SELECT
        JSON_VALUE(json, '$.id') AS ids,
        DENSE_RANK() OVER(PARTITION BY date ORDER BY timestamp DESC) as denseRank
        FROM (
            SELECT *
            FROM report_data_time_series rdts
            WHERE JSON_VALUE(json, '$.reportDataType') = 'EntityReportData'
        ) duplicates
        ORDER BY date DESC, timestamp DESC
    ) dense_ranked
    WHERE denseRank != 1
);

DELETE FROM report_data_time_series
WHERE JSON_VALUE(json, '$.id') IN (
    SELECT ids FROM (
        SELECT
        JSON_VALUE(json, '$.id') AS ids,
        DENSE_RANK() OVER(PARTITION BY date ORDER BY timestamp DESC) as denseRank
        FROM (
            SELECT *
            FROM report_data_time_series rdts
            WHERE JSON_VALUE(json, '$.reportDataType') = 'WebAnalyticUserActivityReportData'
        ) duplicates
        ORDER BY date DESC, timestamp DESC
    ) dense_ranked
    WHERE denseRank != 1
);

COMMIT;