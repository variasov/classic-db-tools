INSERT INTO tasks (name, value)
VALUES (%(name)s, %(value)s)
RETURNING id;
