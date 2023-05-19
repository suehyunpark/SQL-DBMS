create table students ( id char (10) not null, name char (20), primary key (id)
);

create table lectures ( id int not null,
name char (20), capacity int,
primary key (id) );

create table ref (
id int,
foreign key (id) references lectures (id)
);

create table apply (
s_id char (10) not null,
l_id int not null,
apply_date date,
primary key (s_id, l_id),
foreign key (s_id) references students (id), foreign key (l_id) references lectures (id)
);

/* Insert */
-- Insert into students
insert into students (id, name) values('S001', 'John Doe');
insert into students (id, name) values('S002', 'Jane Smith');
insert into students (id, name) values('S003', 'Michael Johnson');
insert into students (id, name) values('S004', 'Emily Davis');
insert into students (id, name) values('S005', 'David Miller');
insert into students (id, name) values('S006', 'Sophia Garcia');
insert into students values('S007', 'Sue Park');
insert into students values('S008', null);
insert into students values('S009', 'Sean Park');
insert into students values('S0010', null);

-- Insert into lectures
insert into lectures (id, name, capacity) values(1, 'Maths 101', 30);
insert into lectures (id, name, capacity) values(2, 'Physics 101', 25);
insert into lectures (id, name, capacity) values(3, 'Chemistry 101', 30);
insert into lectures (id, name, capacity) values(4, 'Biology 101', 35);
insert into lectures (id, name, capacity) values(5, 'English 101', 40);
insert into lectures (id, name, capacity) values(6, 'History 101', 45);
insert into lectures values(7, null, null);
insert into lectures values(8, null, 5);
insert into lectures values(9, 'CS 101', null);

-- Insert into ref
insert into ref (id) values(1);
insert into ref (id) values(2);
insert into ref (id) values(3);
insert into ref (id) values(4);
insert into ref (id) values(5);
insert into ref (id) values(6);

-- Insert into apply
insert into apply (s_id, l_id, apply_date) values('S001', 1, '2023-05-16');
insert into apply (s_id, l_id, apply_date) values('S002', 2, '2023-05-17');
insert into apply (s_id, l_id, apply_date) values('S003', 3, '2023-05-18');
insert into apply (s_id, l_id, apply_date) values('S004', 4, '2023-05-19');
insert into apply (s_id, l_id, apply_date) values('S005', 5, '2023-05-20');
insert into apply (s_id, l_id, apply_date) values('S006', 6, '2023-05-21');
insert into apply values('S007', 7, null);

-- InsertTypeMismatchError
-- 'id' column in 'students' table is of type char but we're trying to insert a number
insert into students (id, name) values(12345, 'John Doe');

-- InsertColumnExistenceError
-- 'non_existent_column' doesn't exist in 'students' table
insert into students (non_existent_column) values('John Doe');

-- InsertColumnNonNullableError
-- 'id' column in 'students' table is non-nullable but we're trying to insert a null value
insert into students (id, name) values(NULL, 'John Doe');

-- InsertDuplicatePrimaryKeyError
-- We're trying to insert a row with a primary key that already exists in 'students' table
insert into students (id, name) values('S001', 'John Doe');

-- InsertReferentialIntegrityError
-- 'l_id' in 'apply' table references 'id' in 'lectures' table. We're trying to insert a value that doesn't exist in 'lectures' table
insert into apply (s_id, l_id, apply_date) values('S001', 999, '2023-05-16');
insert into ref values(null);

/* Delete */
-- DeleteResult
delete from students where id='S001';
delete from lectures where capacity < 30;
delete from students where name='Sue Park';

-- DeleteReferentialIntegrityPassed
delete from lectures where id=1;
delete from students where id='S001';

-- Deleting all rows from a table:
delete from students;

-- Deleting with a condition:
delete from students where id = 'S001';

-- Deleting multiple rows:
delete from students where id < 'S004';

-- Deleting rows with a null value:
delete from students where name is null;

-- Deleting rows with a not null value:
delete from lectures where name is not null;

-- Deleting with multiple conditions:
delete from lectures where capacity > 30 and name != 'History 101';

-- Deleting based on a date:
delete from apply where apply_date <= '2023-05-18';

-- Deleting based on multiple conditions with different operators:
delete from apply where s_id >= 'S005' and l_id != 1 and apply_date > '2023-05-20';

-- Deleting rows where the id is in a specific list:
delete from ref where id > 3 and id <= 5;

-- Deleting using multiple conditions and checking for non-null values:
delete from lectures where capacity >= 30 and name is not null and id != 2 and id < 6;



/* Select */
select name from students;
select name from lectures where capacity>20;
select students.name from students, apply, lectures where students.id=apply.s_id and lectures.id=apply.l_id and lectures.name='Maths 101';

-- SelectTableExistenceError
select * from non_existent_table;

-- SelectColumnResolveError
select non_existent_column from students;

-- WhereIncomparableError
select * from apply where apply_date > 'S001';

-- WhereTableNotSpecified
select students.name from students where lectures.capacity > 20;

-- WhereColumnNotExist
select students.name from students where non_existent_column = 'value';

-- WhereAmbiguousReference
select name from students, lectures where id = 'S001';

-- Selecting all columns from one table:
select * from students;

-- Selecting specific columns from one table:
select id from students;
select students.id from students;

-- Selecting specific columns from multiple tables:
select students.id, students.name, apply.l_id from students, apply where students.id = apply.s_id;

-- Using multiple conditions in the where clause:
select * from lectures where capacity > 30 and name != 'History 101';

-- Checking for null values:
select * from students where name is null;

-- Checking for non-null values:
select * from lectures where name is not null;

-- Using comparison operators in the where clause:
select * from lectures where capacity >= 30;

-- Using multiple tables and conditions in the where clause:
select students.id, students.name, apply.l_id from students, apply where students.id = apply.s_id and apply.l_id > 2;

-- Using all three tables:
select students.id, lectures.id, lectures.name, apply.apply_date from students, lectures, apply where students.id = apply.s_id and lectures.id = apply.l_id and apply_date <= '2023-05-20' and lectures.capacity != 30;

-- Using all operators and all tables:
select students.id, lectures.id, lectures.name, apply.apply_date from students, lectures, apply where students.id = apply.s_id and lectures.id = apply.l_id and apply_date > '2023-05-20' and lectures.capacity < 40 and lectures.name is not null and students.name != 'John Doe';



