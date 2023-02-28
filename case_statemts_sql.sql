
----Sql important to learn---

select * from empy where sal=(select sal from empy order by sal desc limit 1 offset 3);
select deptno,sal,sum(sal) over(order by deptno rows between unbounded preceding and current row) as new_sal from empy;
select sal,DEPTNO,sum(sal) over(order by deptno rows between UNBOUNDED PRECEDING  and unbounded following ) as new_sal from empy;
select sal,sum(sal) over(rows between 1 preceding and 2 following) as new_sal from empy;
--Case statements---
--Condition number1--
select sal, 
case
	WHEN sal > 3000 THEN 'EX_AMOUNT'
    WHEN sal=3000 THEN 'NA_AMOUNT'
    ELSE 'INEX AMONT'
    END AS PRODUCT
    FROM EMPY;
--Condition number 2--
select sal,
CASE
		when sal>1000 then 'audit'
		when sal>=950 then 'expensive'
		when sal=5000 then 'most expensive'
		when sal =300 then 'inexp'
		end as modified_pay
		from empy;
 ------ interview question--
 create table sample2 (sno int);
insert into sample2 values (1),(2),(3),(-1),(-2),(-3);
select * from sample2;
select (case when sno>0 then sno else 0 end) as  rno,
       (case when sno<0 then sno else 0  end) as mno  from sample2;
--Solve this using self join--
select a.id as id1, b.id as id2 from t as a join t as b on a.id = -(b.id) where a.id>0;	
---Interview question--

create table t2 (id int,sal int);
insert into t2 values(1,200),(1,300),(2,400),(2,500),(3,600),(3,800);
select id,concat(w,',',r) as sal from (select a.id,(b.sal)w,(a.sal)r from T2 a join T2 b on a.id=b.id group by id) as q;--Views in my sql--
--Materilized view--
create materialized view sample_view
AS
select * from empy where deptno=30;
--If we delete anything from the query it will not update in order to do that Refresh the query:
REFRESH materialized view sample_view; 
--
CREATE MATERIALIZED VIEW PROJECT.DATASET.MATERIALIZED_VIEW
PARTITION BY RANGE_BUCKET(column_name, buckets)
OPTIONS (enable_refresh = false)
AS SELECT ...
--
CREATE MATERIALIZED VIEW PROJECT.DATASET.MATERIALIZED_VIEW
OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)
AS SELECT ...
--
CREATE MATERIALIZED VIEW  myproject.mydataset.my_mv_table AS (
  SELECT
    product_id,
    SUM(clicks) AS sum_clicks
  FROM
    myproject.mydataset.my_base_table
  GROUP BY
    product_id
);
WITH tmp AS (
  SELECT TIMESTAMP_TRUNC(ts, HOUR) AS ts_hour, *
  FROM mydataset.mytable
)
SELECT ts_hour, COUNT(*) AS cnt
FROM tmp
GROUP BY ts_hour;
--
CREATE TABLE my_project.my_dataset.my_base_table(
  employee_id INT64,
  transaction_time TIMESTAMP)
  PARTITION BY DATE(transaction_time)
  OPTIONS (partition_expiration_days = 2);

CREATE MATERIALIZED VIEW my_project.my_dataset.my_mv_table
  PARTITION BY DATE(transaction_time)
  CLUSTER BY employee_id
AS (
  SELECT
    employee_id,
    transaction_time,
    COUNT(employee_id) AS cnt
  FROM
    my_dataset.my_base_table
  GROUP BY
    employee_id, transaction_time
);
--
CREATE MATERIALIZED VIEW my_project.my_dataset.my_mv_table
  PARTITION BY date
  CLUSTER BY employee_id
AS (
  SELECT
    employee_id,
    _PARTITIONDATE AS date,
    COUNT(1) AS count
  FROM
    my_dataset.my_base_table
  GROUP BY
    employee_id,
    date
);
--Authorized views in bigquery--
1.In the Explorer panel, select the github_source_data dataset.
2.Expand the more_vert Actions option and click Open.
3.Click Sharing, and then select Authorize views.
4.In the Authorized views pane that opens, enter the github_analyst_view view in the Authorized view field.
5.Click Add Authorization.

		   
	   