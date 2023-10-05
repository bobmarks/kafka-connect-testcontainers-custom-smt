create table person (
  id integer,
  name varchar(255),
  updated timestamp without time zone default timezone('utc' :: TEXT, now()),
  primary key (id)
);