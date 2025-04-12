-- Create Customer Dimension table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimCustomer')
CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID VARCHAR(50),
    CustomerSentiment VARCHAR(20),
    IsTopRatedReviewer BIT,
    CustomerRating DECIMAL(3,1),
    TotalReviewsSubmitted INT,
    FirstReviewDate DATE,
    LastReviewDate DATE,
    PreferredCategory VARCHAR(50),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE()
);

-- Populate the Customer dimension with data from CombinedFinalTable
INSERT INTO DimCustomer (
    CustomerID,
    CustomerSentiment,
    IsTopRatedReviewer,
    CustomerRating,
    TotalReviewsSubmitted,
    PreferredCategory
)
SELECT DISTINCT
    author_id AS CustomerID,
    Customer_Sentiment AS CustomerSentiment,
    CASE 
        WHEN Top_Rated = 'Top Rated' THEN 1 
        ELSE 0 
    END AS IsTopRatedReviewer,
    rating AS CustomerRating,
    COUNT(*) AS TotalReviewsSubmitted,
    MAX(primary_category) AS PreferredCategory
FROM CombinedFinalTable
GROUP BY 
    author_id,
    Customer_Sentiment,
    Top_Rated,
    rating;

-- Update review dates
UPDATE d
SET 
    FirstReviewDate = t.FirstReview,
    LastReviewDate = t.LastReview
FROM DimCustomer d
CROSS APPLY (
    SELECT 
        author_id,
        MIN(submission_time) AS FirstReview,
        MAX(submission_time) AS LastReview
    FROM CombinedFinalTable
    GROUP BY author_id
) t
WHERE d.CustomerID = t.author_id;


--REINSERTED CORRECTED DATA

INSERT INTO FactProductReviews (
    DateKey,
    ProductKey,
    CustomerKey,
    Rating,
    ReviewsCount,
    TotalFeedbackCount,
    PositiveFeedbackCount,
    NegativeFeedbackCount,
    IsRecommended
)
SELECT 
    CONVERT(INT, CONVERT(VARCHAR, cft.submission_time, 112)) AS DateKey,
    cft.product_id, -- Now inserting VARCHAR directly
    dc.CustomerKey,
    cft.rating,
    1 AS ReviewsCount,
    ISNULL(cft.total_feedback_count, 0),
    ISNULL(cft.total_pos_feedback_count, 0),
    ISNULL(cft.total_neg_feedback_count, 0),
    ISNULL(CASE WHEN cft.is_recommended = 1 THEN 1 ELSE 0 END, 0) AS IsRecommended
FROM CombinedFinalTable cft
INNER JOIN DimProduct dp ON dp.ProductID = cft.product_id
INNER JOIN DimCustomer dc ON dc.CustomerID = cft.author_id
INNER JOIN DimDate dd ON dd.DateKey = CONVERT(INT, CONVERT(VARCHAR, cft.submission_time, 112));
