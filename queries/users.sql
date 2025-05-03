SELECT 
	u.id as id, 
	u.status as status, 
	u.name as name, 
	u.created_at as created_at, 
	u.updated_at as updated_at, 
	c.code as "country.code", 
	c.name as "country.name"
FROM users u
JOIN countries c ON u.country_id = c.id
WHERE u.updated_at > ?
