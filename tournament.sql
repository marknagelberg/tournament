-- Table definitions for the swiss tournament.
--
-- Put your SQL 'create table' statements in this file; also 'create view'
-- statements if you choose to use it.
--
-- You can write comments in this file by starting them with two dashes, like
-- these lines here.

CREATE DATABASE tournament;
\c tournament;
CREATE TABLE players(id SERIAL PRIMARY KEY, name TEXT, bye BOOLEAN);
--Matches can take 3 possible outcomes - Lose, Draw, Win, or Bye
CREATE TABLE matches(id SERIAL PRIMARY KEY);
CREATE TYPE match_outcome AS ENUM('L', 'D', 'W', 'B');
CREATE TABLE match_outcomes(id SERIAL PRIMARY KEY, match_id INT REFERENCES matches(id), player INT REFERENCES players(id), player_outcome match_outcome);
