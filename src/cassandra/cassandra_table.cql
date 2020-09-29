
USE paroisem_final;

DROP TABLE IF EXISTS flight_by_time;
CREATE TABLE flight_by_time
(
    start_year int,
    start_month int,
    start_day_month int,
    start_day_week int,
    start_hour int,
    cancelled boolean,
    arr_delay int,
    dep_delay int,
    tailnum varchar,
    plane_age int,
    primary key ((start_year, start_month, start_day_month, start_day_week, start_hour), tailnum)
);
