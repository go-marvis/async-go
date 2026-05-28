create table tasks (
    id int not null auto_increment,
    type varchar(255) not null,
    payload text not null,
    headers json not null,
    queue varchar(20) not null,
    retry int not null,
    retried int not null default 0,
    priority tinyint not null default 0,
    timeout int not null default 0,

    status varchar(10) not null,
    result text,

    created_at datetime not null default current_timestamp,
    available_at datetime not null default current_timestamp,
    accepted_at datetime,
    completed_at datetime,

    primary key(id),
    index idx_queue(queue)
);
