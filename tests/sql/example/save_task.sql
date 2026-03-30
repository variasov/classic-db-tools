INSERT INTO tasks (name, value, group_)
VALUES (%(name)s, %(value)s, %(group)s)
RETURNING id;
