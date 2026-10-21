-- Run with: USE ecommerce_dw;

-- Top-N viewed products
SELECT itemid, COUNT(*) AS total_views
FROM sales_fact
WHERE event = 'view'
GROUP BY itemid
ORDER BY total_views DESC
LIMIT 10;

-- Most active visitors
SELECT visitorid, COUNT(*) AS total_events
FROM sales_fact
GROUP BY visitorid
ORDER BY total_events DESC
LIMIT 10;

-- Event distribution
SELECT event, COUNT(*) AS total_events
FROM sales_fact
WHERE event IN ('view','addtocart','transaction')
GROUP BY ROLLUP (event);

-- Behavioral segmentation by item property.
-- Requires the raw item_properties table to be registered in the same database.
SELECT 
e.visitorid,
e.itemid,
ip.property,
ip.value,
COUNT(e.event) AS total_events,
SUM(CASE WHEN e.event = 'transaction' THEN 1 ELSE 0 END) AS purchases
FROM sales_fact e
INNER JOIN item_properties ip ON e.itemid = ip.itemid
GROUP BY e.visitorid, e.itemid, ip.property, ip.value
LIMIT 20;