CREATE TABLE IF NOT EXISTS tasks(
    id serial PRIMARY KEY,
    name varchar NULL,
    value varchar NULL
);

CREATE TABLE IF NOT EXISTS task_status(
    id serial PRIMARY KEY,
    status varchar NULL,
    task_id int NULL
)
