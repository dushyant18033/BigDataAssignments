SELECT COUNT(DISTINCT(pull_requestid)), CAST(time_stamp AS DATE) AS timestamp_date
FROM pullreq_event WHERE ev='discussed'
GROUP BY timestamp_date
ORDER BY timestamp_date;
