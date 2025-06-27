INSERT INTO tasks (id, name)
VALUES ({{ id }}, {{ name }})
RETURNING id;
