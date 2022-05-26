SELECT DATE( DATE(time_stamp) - interval '1 day'*EXTRACT(DOW FROM time_stamp)) AS sunday_of_week,
COUNT(*) AS numPRs
FROM pullreq_event
WHERE ev = 'opened'
GROUP BY sunday_of_week;