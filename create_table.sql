USE pragma_schema;
CREATE TABLE IF NOT EXISTS pragma_table (
time_stamp varchar(10),
price float,
user_id varchar(10)
);

CREATE TABLE IF NOT EXISTS stats_table (
time_stamp varchar(10),
avg_price float,
min_price float,
max_price float
);
