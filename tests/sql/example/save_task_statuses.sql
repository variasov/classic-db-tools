INSERT INTO task_status (status, task_id)
VALUES ({{ status }}, {{ task_id }})
RETURNING id;
