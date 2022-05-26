WITH
months AS
(
	SELECT DISTINCT EXTRACT(MONTH FROM time_stamp) AS date_month
	FROM pullreq_event
),
answers AS
(
	SELECT COUNT(*) AS numPRs, EXTRACT(MONTH FROM time_stamp) AS date_month
	FROM pullreq_event WHERE EXTRACT(YEAR FROM time_stamp)='2010'
	AND ev='merged'
	GROUP BY date_month
)

SELECT months.date_month, COALESCE(answers.numPRs,0)
FROM months
FULL OUTER JOIN answers
ON months.date_month=answers.date_month;