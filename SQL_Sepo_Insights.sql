
--Top 10 Highest Rated Products

SELECT TOP 10 
    product_name, 
    brand_name, 
    AVG(rating) AS avg_rating, 
    COUNT(*) AS review_count
FROM dbo.CombinedfinalTable
WHERE rating IS NOT NULL
GROUP BY product_name, brand_name
HAVING COUNT(*) > 50  
ORDER BY avg_rating DESC;



---------------------Top Rated Brands

SELECT TOP 10 
    brand_name, 
    AVG(rating) AS avg_rating, 
    COUNT(*) AS review_count
FROM dbo.CombinedfinalTable
WHERE rating IS NOT NULL
GROUP BY brand_name
HAVING COUNT(*) > 100  
ORDER BY avg_rating DESC;


-------------------- Products That Received the Most Negative Feedback


SELECT TOP 10 
    product_name, 
    brand_name, 
    SUM(total_neg_feedback_count) AS total_negative_feedback
FROM dbo.CombinedfinalTable
WHERE total_neg_feedback_count IS NOT NULL
GROUP BY product_name, brand_name
ORDER BY total_negative_feedback DESC;



----------

SELECT 
    TopRated, 
    COUNT(*) AS negative_review_count
FROM dbo.CombinedfinalTable
WHERE CustomerSentiment = 'Negative'
GROUP BY TopRated
ORDER BY negative_review_count DESC;

--------

SELECT 
    CASE 
        WHEN price_usd < 20 THEN 'Budget (Under $20)'
        WHEN price_usd BETWEEN 20 AND 50 THEN 'Mid-Range ($20-$50)'
        WHEN price_usd > 50 THEN 'Luxury (Above $50)'
    END AS price_category,
    COUNT(*) AS negative_reviews
FROM dbo.CombinedfinalTable
WHERE CustomerSentiment = 'Negative'
GROUP BY 
    CASE 
        WHEN price_usd < 20 THEN 'Budget (Under $20)'
        WHEN price_usd BETWEEN 20 AND 50 THEN 'Mid-Range ($20-$50)'
        WHEN price_usd > 50 THEN 'Luxury (Above $50)'
    END
ORDER BY negative_reviews DESC;

---------------------- Are Negative Reviews Due to Stock or Availability Issues?


SELECT 
    product_name, 
    brand_name, 
    COUNT(*) AS negative_reviews
FROM dbo.CombinedfinalTable
WHERE CustomerSentiment = 'Negative' AND out_of_stock = 1
GROUP BY product_name, brand_name
ORDER BY negative_reviews DESC;

----------------------- Are Negative Reviews Related to Online-Only Products?

SELECT 
    online_only, 
    COUNT(*) AS negative_reviews
FROM dbo.CombinedfinalTable
WHERE CustomerSentiment = 'Negative'
GROUP BY online_only;


----------------------- Find Products That Are Most Recommended Despite Negative Reviews

SELECT 
    product_name, 
    brand_name, 
    COUNT(*) AS total_negative_reviews,
    SUM(CASE WHEN is_recommended = 1 THEN 1 ELSE 0 END) AS total_recommendations
FROM dbo.CombinedfinalTable
WHERE CustomerSentiment = 'Negative'
GROUP BY product_name, brand_name
ORDER BY total_negative_reviews DESC;



----------Products That Are Mostly Out of Stock

SELECT TOP 10 
    product_name, 
    brand_name, 
    COUNT(*) AS out_of_stock_count
FROM dbo.CombinedfinalTable
WHERE out_of_stock = 1
GROUP BY product_name, brand_name
ORDER BY out_of_stock_count DESC;


-------------Customer Sentiment Analysis

SELECT 
    CustomerSentiment, 
    COUNT(*) AS total_reviews
FROM dbo.CombinedfinalTable
WHERE CustomerSentiment IS NOT NULL
GROUP BY CustomerSentiment
ORDER BY total_reviews DESC;


-----------Sephora Exclusive Products vs. Non-Exclusive

SELECT 
    sephora_exclusive, 
    COUNT(*) AS total_products, 
    AVG(rating) AS avg_rating
FROM dbo.CombinedfinalTable
GROUP BY sephora_exclusive;


--------Most Recommended Products

SELECT TOP 10 
    product_name, 
    brand_name, 
    COUNT(*) AS total_recommendations
FROM dbo.CombinedfinalTable
WHERE is_recommended = 1
GROUP BY product_name, brand_name
ORDER BY total_recommendations DESC;



