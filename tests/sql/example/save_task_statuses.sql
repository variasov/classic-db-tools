INSERT INTO task_status (id, status, task_id)
VALUES ({{ id }}, {{ status }}, {{ task_id }})
RETURNING id;
