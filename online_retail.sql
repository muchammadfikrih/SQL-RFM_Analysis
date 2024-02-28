-- copy online_retail from 'Q:\HDD\dataset\OnlineRetail_II\online_retail_II.csv' DELIMITER ',' CSV HEADER;

--Data Cleaning 
--Checking null-values

--BUAT NGUBAH DTYPE
-- ALTER TABLE online_retail_clean
-- ALTER COLUMN customer_id TYPE INT
-- USING customer_id::integer;
-- select customer_id from online_retail_clean

with null_value as
(
select * 
from online_retail_raw 
where customer_id is not null
)
--Checking duplicate values
, duplicate_check as
(
select *, row_number() over (partition by invoice, stock_code, quantity, price, customer_id, country order by invoice_date)duplicate_label
from null_value
order by invoice_date desc, invoice, stock_code, quantity, price, customer_id, country
)
--Some of the records are negative value. We need to handle this too by removing them.
, negative_value as
(
select * 
from duplicate_check
where duplicate_label = 1 and quantity > 0 and price > 0
)
--Here is our clean data
select * 
into online_retail_clean
from negative_value

alter table online_retail_clean
drop column duplicate_label

---------BEGIN ANALYZING THE DATA--------------------

---------------------Customer Analysis--------------------------------
--Top 10 total purchase by value

with total as
(
select stock_code, description, customer_id, price, quantity, price * quantity as total_purchase, country  
from online_retail_clean
)
, totalpurchase as
(
select customer_id, sum(total_purchase) total_purchase_value
from total
group by customer_id
order by sum(total_purchase) desc
)
,totalpurchase2 as
(
select distinct on (tp.customer_id)tp.customer_id, tp.total_purchase_value, orc.country 
from totalpurchase tp
left join online_retail_clean orc
on tp.customer_id = orc.customer_id
)
select * from totalpurchase2
order by total_purchase_value desc
limit 10

--Top 10 minimum customer ID purchase by value

with total as
(
select stock_code, description, customer_id, price, quantity, price * quantity as total_purchase, country  
from online_retail_clean
)
, totalpurchase as
(
select customer_id, sum(total_purchase) total_purchase_value
from total
group by customer_id
order by sum(total_purchase) desc
)
,totalpurchase2 as
(
select distinct on (tp.customer_id)tp.customer_id, tp.total_purchase_value, orc.country 
from totalpurchase tp
left join online_retail_clean orc
on tp.customer_id = orc.customer_id
)
select * from totalpurchase2
order by total_purchase_value
limit 10

--Top 10 customers by average purchasement value

with total as
(
select stock_code, description, customer_id, price, quantity, price * quantity as total_purchase, country  
from online_retail_clean
)
, totalpurchase as
(
select customer_id, avg(total_purchase) total_purchase_value
from total
group by customer_id
order by sum(total_purchase) desc
)
,totalpurchase2 as
(
select distinct on (tp.customer_id)tp.customer_id, tp.total_purchase_value, orc.country 
from totalpurchase tp
left join online_retail_clean orc
on tp.customer_id = orc.customer_id
)
select * from totalpurchase2
order by total_purchase_value desc
limit 10

-- Top 10 country with most total transaction

with total as
(
select stock_code, description, customer_id, price, quantity, price * quantity as total_purchase, country  
from online_retail_clean
)
select country, sum(total_purchase) total_purchase_value
from total
group by country
order by sum(total_purchase) desc
limit 10

-- Top 10 country with most users


select count(distinct customer_id) total_users , country
from online_retail_clean
group by country
order by count(distinct customer_id) desc
limit 10

---------------------------Recency, Frequency, Monetary------------------------
--

select customer_id, country, invoice, stock_code, description, price, quantity, price * quantity as total_purchase, invoice_date  
into rfm_table
from online_retail_clean

with rfm as
(
select 
	customer_id, 
	sum(total_purchase) monetary_value, 
	avg(total_purchase) avg_monetary_value,
	count(invoice) frequency,
	max(invoice_date) last_order_date,
	(select max(invoice_date) from rfm_table) max_order_period,
	extract(day from '2011-12-09'::timestamp - max(invoice_date)) recency
from rfm_table
group by customer_id
)
, rfm_sum as
(
select 
	*,
	ntile(4) over (order by recency desc) rfm_recency,
	ntile(4) over (order by frequency) rfm_frequency,
	ntile(4) over (order by monetary_value) rfm_monetary
from rfm 
)

select 
	*,
	rfm_recency + rfm_frequency + rfm_monetary as rfm_score,
	cast(rfm_recency as varchar) || cast(rfm_frequency as varchar) || cast (rfm_monetary as varchar) as rfm_score_str
into rfm_score
from rfm_sum

---------------------------Customer Segmentation--------------------

select 
	customer_id, recency, frequency, monetary_value monetary,
	rfm_recency recency_score, rfm_frequency frequency_store, rfm_monetary monetary_score,	
	case 
		when rfm_score_str in('444','443','434','433') then 'churned best customer' --they have transacted a lot and frequent but it has been a long time since last transaction
		when rfm_score_str in('421','422','423','424','434','432','433','431') then 'lost customer'
		when rfm_score_str in('342','332','341','331') then 'declining customer'
		when rfm_score_str in('344','343','334','333') then 'slipping best customer'--they are best customer that have not purchased in a while
		when rfm_score_str in('142','141','143','131','132','133','242','241','243','231','232','233') then 'active loyal customer' -- they have purchased recently, frequently, but have low monetary value
		when rfm_score_str in('112','111','113','114','211','213','214','212') then 'new customer' 
		when rfm_score_str in('144') then 'best customer'-- they have purchase recently and frequently, with high monetary value
		when rfm_score_str in('411','412','413','414','313','312','314','311') then 'one time customer'
		when rfm_score_str in('222','221','223','224') then 'Potential customer'
		else 'customer'
	end rfm_segment
into customer_segmented
from rfm_score

select distinct cs.*, orc.country
into customer_segmented_country
from customer_segmented cs
join online_retail_clean orc
on cs.customer_id = orc.customer_id

select rfm_segment, count(customer_id), country 
from customer_segmented_country
group by rfm_segment, country
order by country


select * from rfm_table