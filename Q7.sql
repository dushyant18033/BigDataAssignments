SELECT COUNT(*) AS num_PRs, author FROM pullreq_event
WHERE ev='opened' AND EXTRACT(YEAR FROM time_stamp)='2011'
GROUP BY author
ORDER BY num_PRs DESC LIMIT 1;
