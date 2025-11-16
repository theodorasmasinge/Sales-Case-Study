WITH
-- BASE DATA FROM RAW SALES TABLE

base_data AS (
    SELECT
        TO_DATE(Date, 'DD/MM/YYYY') AS SaleDate,
        Sales,
        COST_OF_SALES,
        QUANTITY_SOLD,
        (Sales / NULLIF(QUANTITY_SOLD, 0)) AS PricePerUnit,
        (COST_OF_SALES / NULLIF(QUANTITY_SOLD, 0)) AS CostPerUnit,
        (Sales - COST_OF_SALES) AS GrossProfit
    FROM "SALESCASESTUDY"."SALES"."THEO"
),


--  DAILY AGGREGATION (TOTAL SALES, TOTAL UNITS, DAILY PRICE)

daily_prices AS (
    SELECT
        SaleDate,
        SUM(Sales) AS Total_Sales,
        SUM(Quantity_Sold) AS Total_Quantity,
        ROUND(SUM(Sales) / NULLIF(SUM(Quantity_Sold), 0), 2) AS PricePerUnit
    FROM base_data
    GROUP BY SaleDate
),


--  AVERAGE DAILY PRICE (USED TO FLAG PROMOS
avg_price AS (
    SELECT AVG(PricePerUnit) AS AvgPrice
    FROM daily_prices
),


--  FLAG PROMO DAYS (10% BELOW AVG PRICE)

promo_flags AS (
    SELECT
        d.*,
        CASE WHEN d.PricePerUnit < a.AvgPrice * 0.90 THEN 1 ELSE 0 END AS IsPromo
    FROM daily_prices d
    CROSS JOIN avg_price a
),


-- START OF EACH PROMO SEQUENCE
promo_markers AS (
    SELECT
        *,
        CASE
            WHEN LAG(IsPromo) OVER (ORDER BY SaleDate) = 0 AND IsPromo = 1 THEN 1
            WHEN LAG(IsPromo) OVER (ORDER BY SaleDate) IS NULL AND IsPromo = 1 THEN 1
            ELSE 0
        END AS NewGroup
    FROM promo_flags
),


--   PROMO GROUP IDs
promo_groups AS (
    SELECT
        *,
        SUM(NewGroup) OVER (ORDER BY SaleDate) AS PromoGroupID
    FROM promo_markers
),


--  GET TOP 3 LONGEST PROMO PERIOD
promo_periods AS (
    SELECT
        MIN(SaleDate) AS PromoStart,
        MAX(SaleDate) AS PromoEnd,
        COUNT(*) AS PromoDays
    FROM promo_groups
    WHERE IsPromo = 1
    GROUP BY PromoGroupID
    ORDER BY PromoDays DESC
    LIMIT 3
),


--  ELASTICITY CALCULATIONS FOR EACH PROMO PERIOD

elasticity AS (
    SELECT
        p.PromoStart,
        p.PromoEnd,
        p.PromoDays,

        (SELECT AVG(PricePerUnit)
         FROM promo_flags
         WHERE SaleDate BETWEEN p.PromoStart AND p.PromoEnd) AS PromoAvgPrice,

        (SELECT AVG(Total_Quantity)
         FROM promo_flags
         WHERE SaleDate BETWEEN p.PromoStart AND p.PromoEnd) AS PromoAvgQty,

        
        (SELECT AVG(PricePerUnit)
         FROM promo_flags
         WHERE SaleDate BETWEEN DATEADD(day, -7, p.PromoStart) AND DATEADD(day, -1, p.PromoStart)) AS PreAvgPrice,

        (SELECT AVG(Total_Quantity)
         FROM promo_flags
         WHERE SaleDate BETWEEN DATEADD(day, -7, p.PromoStart) AND DATEADD(day, -1, p.PromoStart)) AS PreAvgQty
    FROM promo_periods p
),


final_elasticity AS (
    SELECT
        PromoStart,
        PromoEnd,
        PromoDays,
        ROUND(
            ((PromoAvgQty - PreAvgQty) / NULLIF(PreAvgQty, 0)) /
            ((PromoAvgPrice - PreAvgPrice) / NULLIF(PreAvgPrice, 0)),
            2
        ) AS Price_Elasticity
    FROM elasticity
),

final_daily_output AS (
    SELECT
        b.SaleDate,
        EXTRACT(YEAR FROM b.SaleDate) AS Year,
        TO_CHAR(b.SaleDate, 'Mon') AS MonthName,
        b.Sales,
        b.COST_OF_SALES,
        b.QUANTITY_SOLD,
        ROUND(b.Sales / NULLIF(b.QUANTITY_SOLD, 0), 2) AS Sales_Per_Unit,
        CASE WHEN p.IsPromo = 1 THEN 'Promo Day' ELSE 'Regular Day' END AS SalesType,
        b.GrossProfit AS Daily_GrossProfit,
        p.PricePerUnit AS Daily_SalesPricePerUnit,
        AVG(p.PricePerUnit) OVER (
            PARTITION BY EXTRACT(YEAR FROM b.SaleDate),
                         EXTRACT(MONTH FROM b.SaleDate)
        ) AS Average_Unit_Sales_Price
    FROM base_data AS b
    LEFT JOIN promo_flags p ON b.SaleDate = p.SaleDate
)

SELECT *
FROM final_daily_output
ORDER BY SaleDate;
