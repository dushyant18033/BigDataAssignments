Select COUNT(ev), CAST(time_stamp AS DATE) AS timestamp_date
FROM pullreq_event
GROUP BY timestamp_date
ORDER BY timestamp_date
