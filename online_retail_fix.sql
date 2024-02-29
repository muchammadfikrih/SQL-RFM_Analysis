-- copy online_retail from 'Q:\HDD\dataset\OnlineRetail_II\online_retail_II.csv' DELIMITER ',' CSV HEADER;

---------------Data Cleaning--------------- 
---------------Checking null-values-----------------

with null_value as
(
select * 
from online_retail_raw 
where customer_id is not null
)
---------------Checking duplicate values-------------
, duplicate_check as
(
select *, row_number() over (partition by invoice, stock_code, quantity, price, customer_id, country order by invoice_date)duplicate_label
from null_value
order by invoice_date desc, invoice, stock_code, quantity, price, customer_id, country
)
---------------Some of the records are negative value. We don't want this recordes in our analysis. So we will drop this records too.
, negative_value as
(
select * 
from duplicate_check
where duplicate_label = 1 and quantity > 0 and price > 0
)
---------------Here is our clean data----------------
select invoice, stock_code, description, quantity, invoice_date, price, customer_id, country 
into online_retail_clean
from negative_value

select * from online_retail_clean

---------------BEGIN ANALYZING THE DATA--------------------

---------------Top 10 country with most total transaction---------------

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

---------------Top 10 country with most users---------------


select count(distinct customer_id) total_users , country
from online_retail_clean
group by country
order by count(distinct customer_id) desc
limit 10

---------------Recency, Frequency, Monetary------------------------

---------------Add column total purchase and re-arrange the column for analysis--------
select customer_id, country, invoice, invoice_date, stock_code, description, price, quantity, price * quantity as total_purchase  
into rfm_table
from online_retail_clean

select * from rfm_table

---------------Begin customers scoring------------
-------------------------------------------------------------------------------------------------------------------------------------------------
--We need 3 variables to do this kind analysis.
--First is recency, which tells how recent was the customer's last purchase. So we need the last transaction recorded-
--as the last period of analysis and we need the last purchase done by the customer. 

--Second, we need frequency, which tells how often the customer make a purchase in a given period.
--Third is monetary, which tells how much money did the customer spend in this given period.
-------------------------------------------------------------------------------------------------------------------------------------------------

with rfm as 
(
select 
	customer_id, 
	sum(total_purchase) monetary_value, 
	count(invoice) frequency,
	max(invoice_date) last_order_date,
	(select max(invoice_date) from rfm_table) max_order_period,
	extract(day from '2011-12-09'::timestamp - max(invoice_date)) recency
from rfm_table
group by customer_id
)
, scoring as --scoring the customers by making quantile
(
select 
	*,
	ntile(4) over (order by recency desc) rfm_recency,
	ntile(4) over (order by frequency) rfm_frequency,
	ntile(4) over (order by monetary_value) rfm_monetary
from rfm 
)
select --Cast the score to varchar to tell customer's score on each aspect 
	*,
	cast(rfm_recency as varchar) || cast(rfm_frequency as varchar) || cast (rfm_monetary as varchar) as rfm_score
into rfm_table_raw
from scoring

select * from rfm_table_raw

---------------------------Customer Segmentation--------------------
-------------------------------------------------------------------------------------------------------------------------------------------------
--So we already know the customer's score. Next we need to group these customers based on scoring condition. In this case we group-
--the customer into these labels with certain criteria:
--		Customer: these are average customers
--		One Time Customers: these are customers that made one purchase a long time ago
--		Active Loyal Customers: they are our customers that make good purchases frequently and have a good recency score.
--		Slipping Best Customer: they are the best customers that have not made any purchase recently.
--		Potential Customers: they are customers who made a big purchase recently.
--		Best Customers: they are the perfect customers with a high frequency of purchases, they have purchased recently and purchased a lot.
--		Churned Best customer: they are the best customers who have not made any purchases for a long time and churned.
--		New Customers: they have made a very recent purchase and have a low-frequency score.
--		Lost Customers: they are normal and loyal customers who have not made any purchases in a very long time.
--		Declining Customers: they are customers who have not made a purchase recently.
-------------------------------------------------------------------------------------------------------------------------------------------------
select 
	customer_id, recency, frequency, monetary_value monetary,
	rfm_recency recency_score, rfm_frequency frequency_score, rfm_monetary monetary_score,	
	case 
		when rfm_score in('144','143','134','133') then 'Churned Best Customer' --they have transacted a lot and frequent but it has been a long time since last transaction
		when rfm_score in('121','122','123','124','131','132','133','134') then 'Lost Customer'
		when rfm_score in('242','232','241','231') then 'Declining Customer'
		when rfm_score in('244','243','234','233') then 'Slipping Best Customer'--they are best customer that have not purchased in a while
		when rfm_score in('441','442','443','431','432','433','341','342','343','331','332','333') then 'Active Loyal Customer' -- they have purchased recently, frequently, but have low monetary value
		when rfm_score in('411','412','413','414','311','312','313','314') then 'New Customer' 
		when rfm_score in('444') then 'Best Customer'-- they have purchase recently and frequently, with high monetary value
		when rfm_score in('111','112','113','114','211','212','213','214') then 'One Time Customer'
		when rfm_score in('321','322','323','324') then 'Potential Customer'
		else 'Customer'
	end rfm_segment
into customer_segmented
from rfm_table_raw

--Add country column and store the result on final table for visualisation
select distinct on (cs.customer_id) cs.customer_id, recency, frequency, monetary, 
				recency_score, frequency_score, monetary_score, rfm_segment, orc.country
into customer_segmented_country
from customer_segmented cs 
join online_retail_clean orc
on cs.customer_id = orc.customer_id

select * from customer_segmented_country
