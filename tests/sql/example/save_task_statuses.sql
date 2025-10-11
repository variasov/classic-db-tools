INSERT INTO task_status (title, task_id)
VALUES (%(title)s, %(task_id)s)
RETURNING id;
