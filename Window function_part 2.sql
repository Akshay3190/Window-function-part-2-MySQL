CREATE DATABASE window_part_2;
USE window_part_2;
CREATE TABLE Product 
(    product_category VARCHAR (255),
	 brand VARCHAR (255),
     product_name VARCHAR (255),
     price int 
);

INSERT INTO product VALUES
('Phone', 'Apple', 'iPhone 12 Pro Max', 1300),
('Phone', 'Apple', 'iPhone 12 Pro', 1100),
('Phone', 'Apple', 'iPhone 12', 1000),
('Phone', 'Samsung', 'Galaxy Z Fold 3', 1800),
('Phone', 'Samsung', 'Galaxy Z Flip 3', 1000),
('Phone', 'Samsung', 'Galaxy Note 20', 1200),
('Phone', 'Samsung', 'Galaxy S 21', 1000),
('Phone', 'OnePlus', 'OnePlus Nord', 300),
('Phone', 'OnePlus', 'OnePlus 9', 800),
('Phone', 'Google', 'Pixel 5', 600),
('Laptop', 'Apple', 'MacBook Pro 13', 2000),
('Laptop', 'Apple', 'MacBook Air', 1200),
('Laptop', 'Microsoft', 'Surface Laptop 4', 2100),
('Laptop', 'Dell', 'XPS 13', 2000),
('Laptop', 'Dell', 'XPS 15', 2300),
('Laptop', 'Dell', 'XPS 17', 2500),
('Earphone', 'Apple', 'AirPods Pro', 280),
('Earphone', 'Samsung', 'Galaxy Buds Pro', 220),
('Earphone', 'Samsung', 'Galaxy Buds Live', 170),
('Earphone', 'Sony', 'WF-1000XM4', 250),
('Headphone', 'Sony', 'WH-1000XM4', 400),
('Headphone', 'Apple', 'AirPods Max', 550),
('Headphone', 'Microsoft', 'Surface Headphones 2', 250),
('Smartwatch', 'Apple', 'Apple Watch Series 6', 1000),
('Smartwatch', 'Apple', 'Apple Watch SE', 400),
('Smartwatch', 'Samsung', 'Galaxy Watch 4', 600),
('Smartwatch', 'OnePlus', 'OnePlus Watch', 220);

SELECT * FROM product;

-- FIRST_VALUE
-- Write query to display the most expensive product under each category (corresponding to each record)
SELECT p.*,
first_value (product_name) OVER (partition by product_category ORDER BY price DESC) as most_exp_product
FROM product p;

-- LAST_VALUE 
-- Write query to display the least expensive product under each category (corresponding to each record)
SELECT p.*,
first_value (product_name) OVER (partition by product_category ORDER BY price DESC) as most_exp_product,
last_value (product_name) OVER (partition by product_category ORDER BY price DESC
 range between unbounded preceding and unbounded following) as least_exp_product
FROM product p;


-- Write query to display the least expensive product in 'Phone' category (corresponding to each record)
SELECT p.*,
first_value (product_name) OVER (partition by product_category ORDER BY price DESC) as most_exp_product,
last_value (product_name) OVER (partition by product_category ORDER BY price DESC
 range between unbounded preceding and unbounded following) as least_exp_product
FROM product p
WHERE product_category = 'Phone';

-- Alternate way to write SQL query using Window functions for  'Phone' category
SELECT p.*,
FIRST_VALUE (product_name) OVER w as most_exp_prodcut,
LAST_VALUE (product_name) OVER w as least_exp_product
FROM product p
WHERE product_category = 'Phone'
WINDOW w as (partition by product_category ORDER BY price DESC
             range between unbounded preceding and unbounded following);

-- NTH_VALUE 
-- Write query to display the Second most expensive product under each category.
SELECT p.*,
FIRST_VALUE (product_name) OVER w as most_exp_prodcut,
LAST_VALUE (product_name) OVER w as least_exp_product,
NTH_VALUE (product_name,2) OVER w as second_most_exp_product
FROM product p
WINDOW w as (partition by product_category ORDER BY price DESC
             range between unbounded preceding and unbounded following);

-- NTILE
-- Write a query to segregate all the expensive phones, mid range phones and the cheaper phones.

SELECT p.*,
NTILE (3) OVER (ORDER BY price DESC ) as BUCKETS
FROM product p
WHERE product_category = 'Phone';


SELECT x.product_name, x.price,x.buckets,
case WHEN x.buckets = 1 THEN 'Expensive Phones'
     WHEN x.buckets = 2 THEN 'Mid Range Phones'
     WHEN x.buckets = 3 THEN 'Cheaper Phones' END as Phone_Category
FROM (
	 SELECT p.*,
     NTILE (3) OVER (ORDER BY price DESC) as buckets
     FROM product p
     WHERE product_category = 'Phone' ) as x;


-- CUME_DIST (cumulative distribution) ; 
/*  Formula = Current Row no (or Row No with value same as current row) / Total no of rows */

-- Query to fetch all products which are constituting the first 30% 
-- of the data in products table based on price.

SELECT p.*,
CUME_DIST () OVER (ORDER BY price DESC) as cume_distribution,
ROUND (CUME_DIST () OVER (ORDER BY price DESC) * 100,2) as cume_dist_Percentage
FROM product p;

SELECT *
FROM (
SELECT *,
ROUND (CUME_DIST () OVER (ORDER BY price DESC)*100,2) as cume_dist_percentage
FROM product ) x
WHERE x.cume_dist_percentage <=30;

SELECT product_name, CONCAT_WS (' ',cume_dist_percentage,'%') as cume_dist_percentage
FROM (
SELECT *,
CUME_DIST () OVER (ORDER BY price DESC) as  cume_distribution,
ROUND (CUME_DIST () OVER (ORDER BY price DESC)*100,3) as cume_dist_percentage
FROM product ) x
WHERE x.cume_dist_percentage <30;



-- PERCENT_RANK (relative rank of the current row / Percentage Ranking)
/* Formula = Current Row No - 1 / Total no of rows - 1 */

-- Query to identify how much percentage more expensive is "Galaxy Z Fold 3" when compared to all products.
SELECT *,
PERCENT_RANK () OVER (ORDER BY price) as percentage_rank,
ROUND (PERCENT_RANK () OVER (ORDER BY price) * 100,2) as  per
FROM product;

select product_name, CONCAT_WS (' ',per,'%') as percentage
from (
    select *,
    percent_rank() over(order by price) ,
    round(percent_rank() over(order by price) * 100, 2) as per
    from product) x
where x.product_name='Galaxy Z Fold 3';

SELECT ROUND((21/26) *100,2) as Result;




      
