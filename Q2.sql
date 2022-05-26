WITH 
	tmp as (SELECT EXTRACT(MONTH FROM time_stamp) as date_month, 
		   author, count(*) as numPRs
		   FROM pullreq_event
		   WHERE ev = 'discussed'
		   GROUP BY date_month, author)

SELECT * FROM tmp
WHERE (date_month, numPRs) IN 
( 
	SELECT date_month, MAX(numPRs)
	FROM tmp
	GROUP BY date_month
);
