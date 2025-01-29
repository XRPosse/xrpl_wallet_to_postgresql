-- Find the wallet funding wallets
SELECT DISTINCT sender_wallet
FROM your_table_name
WHERE action = 'xrp_receive';

-- Find total XRP received and total XRP sent
SELECT 
    SUM(CASE WHEN action = 'xrp_receive' THEN xrp_value ELSE 0 END) AS total_xrp_receive,
    SUM(CASE WHEN action = 'xrp_payment' THEN xrp_value ELSE 0 END) AS total_xrp_payment
FROM your_table_name
WHERE action IN ('xrp_receive', 'xrp_payment');

-- Select all entries 
SELECT * FROM your_table_name;
ORDER BY action ASC 