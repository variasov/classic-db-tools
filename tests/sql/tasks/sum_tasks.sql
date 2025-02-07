SELECT
    SUM(tasks.id)
FROM tasks
JOIN task_status ON task_status.task_id = tasks.id
WHERE
{% if status %} task_status.status LIKE {{ status }} AND {% endif %}
TRUE;
