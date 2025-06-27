SELECT
    tasks.id,
    tasks.name,
    task_status.status
FROM tasks
INNER JOIN task_status ON tasks.id = task_status.task_id
WHERE TRUE
    {% if name %} AND tasks.name LIKE {{ name }} {% endif %}
ORDER BY task_status.task_id;
