SELECT COUNT(*), CAST(time_stamp AS DATE) AS timestamp_date
FROM pullreq_event WHERE ev='opened'
GROUP BY timestamp_date
ORDER BY timestamp_date
