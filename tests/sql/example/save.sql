INSERT INTO tasks(name, value)
VALUES ({{ name }}, {{ value }})
RETURNING id;
