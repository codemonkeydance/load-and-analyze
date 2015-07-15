CREATE TABLE IF NOT EXISTS consumer_complaints  (
	date_received  date,
	product  varchar(50),
	sub_product  varchar(50),
	issue  varchar(50),
	sub_issue  varchar(50),
	consumer_complaint_narrative  varchar,
	company_public_response  varchar,
	company  varchar(100),
	state  varchar(2),
	zip  varchar(5),
	submitted_via  varchar(50),
	date_sent_to_company  date,
	company_response_to_consumer  varchar,
	timely_response  varchar(3),
	consumer_disputed  varchar(3),
	complanint_id  integer,
	PRIMARY  KEY  (complanint_id)
)
WITH  (autovacuum_enabled  =  FALSE,  toast.autovacuum_enabled  =  FALSE);