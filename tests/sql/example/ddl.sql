CREATE TABLE tasks(
    id serial PRIMARY KEY,
    name varchar NULL,
    value varchar NULL
);

CREATE TABLE task_status(
    id serial PRIMARY KEY,
    status varchar NULL,
    task_id int NULL
)
