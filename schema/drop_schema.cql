CREATE KEYSPACE IF NOT EXISTS FlyBook
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE FlyBook;

CREATE TABLE FreeSeats (
	flightId int,
	seat int,
	PRIMARY KEY (flightId, seat)
);

CREATE TABLE OccupiedSeats (
	flightId int,
	seat int,
	customer varchar,
	PRIMARY KEY (flightId, seat)
);

CREATE TABLE CustomersReservations (
	customer varchar,
	flightId int,
	seat int,
	PRIMARY KEY (customer, flightId, seat)
);

CREATE TABLE Airplanes (
	name varchar,
	seatCount int,
	PRIMARY KEY (name)
);

CREATE TABLE Flights (
	id int,
	name varchar,
	origin varchar,
	destination varchar,
	PRIMARY KEY (id)

);

insert into airplanes (name, seatCount) values ('A380', 255);
insert into airplanes (name, seatCount) values ('737', 120);
insert into airplanes (name, seatCount) values ('GS', 33);

