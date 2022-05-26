WITH
tmp AS
(
	SELECT DATE( DATE(time_stamp) - interval '1 day'*EXTRACT(DOW FROM time_stamp)) AS sunday_of_week,
	COUNT(*) AS numPRs, author
	FROM pullreq_event
    WHERE ev = 'discussed'
    GROUP BY sunday_of_week, author
)

SELECT * FROM tmp
WHERE (sunday_of_week, numPRs) IN 
( 
	SELECT sunday_of_week, MAX(numPRs)
	FROM tmp
	GROUP BY sunday_of_week
);