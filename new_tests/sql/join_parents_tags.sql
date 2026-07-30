SELECT p.id AS parent__id,
       p.name AS parent__name,
       t.name AS tag__name
FROM _map_parents p
JOIN _map_tags t ON t.parent_id = p.id
ORDER BY t.name
