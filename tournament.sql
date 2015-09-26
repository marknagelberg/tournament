-- Table definitions for the swiss tournament.
--
-- Put your SQL 'create table' statements in this file; also 'create view'
-- statements if you choose to use it.

CREATE DATABASE tournament;
\c tournament;
CREATE TABLE players(id SERIAL PRIMARY KEY, name TEXT, bye BOOLEAN);
--Matches can take 3 possible outcomes - Lose, Draw, Win, or Bye
CREATE TABLE matches(id SERIAL PRIMARY KEY);
CREATE TYPE match_outcome AS ENUM('L', 'D', 'W', 'B');
CREATE TABLE match_outcomes(id SERIAL PRIMARY KEY, match_id INT REFERENCES matches(id), player INT REFERENCES players(id), player_outcome match_outcome);
--Create table consisting of player, matches, wins, draws, and
--opponent match wins (omw)
 CREATE VIEW mwd_table AS
                 SELECT id, name,
                  (SELECT count(*)
                   FROM match_outcomes
                   WHERE match_outcomes.player = players.id) AS matches,
                  (SELECT count(*)
                   FROM match_outcomes
                   WHERE match_outcomes.player = players.id
                         AND match_outcomes.player_outcome = 'W') AS wins,
                  (SELECT count(*)
                   FROM match_outcomes
                   WHERE match_outcomes.player = players.id
                         AND match_outcomes.player_outcome = 'D') AS draws,
                  (SELECT count(*)
                   FROM match_outcomes
                   WHERE match_outcomes.player = players.id
                         AND match_outcomes.player_outcome = 'B') AS byes,
                  (SELECT count(m3.player)
                   FROM match_outcomes AS m1,
                        match_outcomes AS m2,
                        match_outcomes AS m3
                   WHERE m1.player = players.id
                         AND m1.player != m2.player
                         AND m1.match_id = m2.match_id
                         AND m2.player = m3.player
                         AND m3.player_outcome = 'W'
                   GROUP BY m1.player) AS omw
                 FROM players;

--SET omw = 0 whenever it's empty.
--UPDATE mwd_table SET omw = 0 WHERE omw = NULL;

--Add a ranking field that ranks players so that wins are worth 3 points, byes are worth 3 points
--and draws are worth 1 point. Note that the view below was inspired by the responses to the 
--following stackoverflow thread I started:
--http://stackoverflow.com/questions/32766068/how-to-select-distinct-records-across-two-self-joined-columns
CREATE VIEW ranked_players AS
       SELECT *, rank() OVER (order by mwd_table.wins*3 + mwd_table.byes*3 + mwd_table.draws, mwd_table.id) as player_rank
       FROM mwd_table
