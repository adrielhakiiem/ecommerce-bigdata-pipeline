-- Top-N products
SELECT itemid, COUNT(*) AS total_views
FROM events
WHERE event = 'view'
GROUP BY itemid
ORDER BY total_views DESC
LIMIT 10;

-- User segmentation
SELECT visitorid, COUNT(*) AS total_events
FROM events
GROUP BY visitorid
ORDER BY total_events DESC
LIMIT 10;

-- Event distribution
SELECT event, COUNT(*) AS total_events
FROM events
WHERE event IN ('view','addtocart','transaction')
GROUP BY event WITH ROLLUP;

-- Behavioral segmentation
SELECT 
e.visitorid,
e.itemid,
ip.property,
ip.value,
COUNT(e.event) AS total_events,
SUM(CASE WHEN e.event = 'transaction' THEN 1 ELSE 0 END) AS purchases
FROM events e
INNER JOIN item_properties ip ON e.itemid = ip.itemid
GROUP BY e.visitorid, e.itemid, ip.property, ip.value
LIMIT 20;