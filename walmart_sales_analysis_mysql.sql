CREATE DATABASE walmart_sales_db;
USE walmart_sales_db;

SELECT *
FROM walmart_sales;

-- count of payment methods
SELECT count( payment_method)
FROM walmart_sales;

-- Count payment methods and number of transactions by payment method
SELECT  payment_method, count(payment_method)
FROM walmart_sales
group by payment_method;

-- Count distinct branches
SELECT count(distinct branch)
FROM walmart_sales;

---- Find the maximum and minimum quantity sold
SELECT max(quantity), min(quantity)
FROM walmart_sales;

-- Business Problems
 -- Q1: Find different payment methods, number of transactions, and quantity sold by payment method
SELECT payment_method, count(payment_method) AS transactions, sum(quantity) AS no_quantity_sold
FROM walmart_sales
GROUP BY payment_method;

-- #2: Identify the highest-rated category in each branch
-- Display the branch, category, and avg rating
SELECT branch, category, avg_rating, rnk
FROM (
    SELECT
        branch,
        category,
        avg_rating,
        RANK() OVER (PARTITION BY branch ORDER BY avg_rating DESC) AS rnk
    FROM (
        SELECT 
            branch,
            category,
            AVG(rating) AS avg_rating
        FROM walmart_sales
        GROUP BY branch, category
    ) AS t
) AS ranked
WHERE rnk =1;

-- Q3: Calculate the total quantity of items sold per payment method
SELECT 
    payment_method,
    SUM(quantity) AS no_qty_sold
FROM walmart_sales
GROUP BY payment_method;

-- Q4: Determine the average, minimum, and maximum rating of categories for each city
SELECT 
    city,
    category,
    MIN(rating) AS min_rating,
    MAX(rating) AS max_rating,
    AVG(rating) AS avg_rating
FROM walmart_sales
GROUP BY city, category;

-- Q5: Calculate the total profit and revenue for each category
SELECT 
    category,
    ROUND(SUM(total_price),2) as total_revenue,
    ROUND(SUM(unit_price * quantity * profit_margin),2) AS total_profit
FROM walmart_sales
GROUP BY category
ORDER BY total_profit DESC;

-- Q6: Determine the most common payment method for each branch
WITH cte AS (
    SELECT 
        branch,
        payment_method,
        COUNT(*) AS total_trans,
        RANK() OVER(PARTITION BY branch ORDER BY COUNT(*) DESC) AS rnk
    FROM walmart_sales
    GROUP BY branch, payment_method
)
SELECT 
    branch, 
    payment_method AS preferred_payment_method, total_trans, rnk
FROM cte
WHERE rnk = 1;

-- Q7: Categorize sales into Morning, Afternoon, and Evening shifts
SELECT
    branch,
    CASE 
        WHEN HOUR(TIME(time)) < 12 THEN 'Morning'
        WHEN HOUR(TIME(time)) BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS shift,
    COUNT(*) AS no_trans
FROM walmart_sales
GROUP BY branch, shift
ORDER BY branch, no_trans DESC;

-- Q8) Identify the highest-rated category in each branch
-- Display the branch, category, and avg rating
SELECT branch, category, avg_rating, rnk
FROM (
    SELECT 
        branch,
        category,
        AVG(rating) AS avg_rating,
        RANK() OVER(PARTITION BY branch ORDER BY AVG(rating) DESC) AS rnk
    FROM walmart_sales
    GROUP BY branch, category
) AS ranked
WHERE rnk = 1;

-- Q9)Find the top revenue-generating category of every branch.
SELECT branch, category, total_revenue
FROM (
    SELECT
        branch,
        category,
        SUM(unit_price * quantity) AS total_revenue,
        RANK() OVER(PARTITION BY branch ORDER BY SUM(unit_price * quantity) DESC) AS rnk
    FROM walmart_sales
    GROUP BY branch, category
) AS ranked
WHERE rnk = 1;

-- Q10)Find which hour of the day had maximum number of transactions in each branch.
SELECT branch, hour_of_day, total_trans
FROM (
    SELECT 
        branch,
        HOUR(time) AS hour_of_day,
        COUNT(*) AS total_trans,
        RANK() OVER(PARTITION BY branch ORDER BY COUNT(*) DESC) AS rnk
    FROM walmart_sales
    GROUP BY branch, hour_of_day
) AS ranked
WHERE rnk = 1;

-- Q11) Find customers with multiple transactions.
SELECT customer_id, COUNT(*) AS visits
FROM walmart_sales
GROUP BY customer_id
HAVING COUNT(*) > 1;


