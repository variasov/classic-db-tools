SELECT p.id AS parent__id,
       p.name AS parent__name,
       c.id AS child__id,
       c.label AS child__label
FROM _map_parents p
LEFT JOIN _map_children c ON c.parent_id = p.id
ORDER BY p.id, c.id
