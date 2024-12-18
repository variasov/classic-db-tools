INSERT INTO tasks (name)
VALUES
{% for task in tasks %}
    ({{ task['name'] }}){% if not loop.last %},{% endif %}
{% endfor %};
