CREATE TABLE pullreq_event
(
	pull_requestid INT,
	author VARCHAR(100),
	ev VARCHAR(50),
	time_stamp TIMESTAMP
); 
	-- Creates an empty SQL TABLE consisting of 4 fields.

COPY pullreq_event FROM 'E:\Downloads\pullreq_events.csv' DELIMITER ',' CSV;
	-- Copies the data from the provided CSV to the SQL TABLE we just created.

SELECT * FROM pullreq_event;
	-- Display the contents of the table created.