-- Table definitions for the tournament project.
--
-- Put your SQL 'create table' statements in this file; also 'create view'
-- statements if you choose to use it.
--
-- You can write comments in this file by starting them with two dashes, like
-- these lines here.

CREATE DATABASE tournament;
\c tournament;
--Idea - set table restrictions ensuring wins <= matches and draws <=
--matches. Also make sure wins, draws, matches >= 0.
CREATE TABLE players(id SERIAL PRIMARY KEY, name TEXT, bye BOOLEAN);
--Matches can take 3 possible outcomes - Lose, Draw, or Win
CREATE TABLE matches(id SERIAL PRIMARY KEY);
CREATE TYPE match_outcome AS ENUM('L', 'D', 'W', 'B');
CREATE TABLE match_outcomes(id SERIAL PRIMARY KEY, match_id INT REFERENCES matches(id), player INT REFERENCES players(id), player_outcome match_outcome);
