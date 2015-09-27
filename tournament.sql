-- Table definitions for the swiss tournament.
-- The database has three core tables:
--   > players table which provides each players name and whether they
--     recieved a bye
--   > matches table which provides an id for each match
--   > match_outcomes table which lists each of the outcomes
--     of the players in the matches. Outcomes include lose (L)
--     draw (D), win (W), and bye (B).

CREATE DATABASE tournament;
\c tournament;
CREATE TABLE players(id SERIAL PRIMARY KEY, name TEXT, bye BOOLEAN);

--Matches can take 3 possible outcomes - Lose, Draw, Win, or Bye
CREATE TABLE matches(id SERIAL PRIMARY KEY);
CREATE TYPE match_outcome AS ENUM('L', 'D', 'W', 'B');
CREATE TABLE match_outcomes(id SERIAL PRIMARY KEY, match_id INT REFERENCES matches(id), player INT REFERENCES players(id), player_outcome match_outcome);

--Create table consisting of player, matches, wins, and draws
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
                         AND match_outcomes.player_outcome = 'B') AS byes
                 FROM players;

--Create view consisting of each player and all of the opponents they have faced.
--If player hasn't faced any opponents, they appear once with a row of nulls.
CREATE VIEW opponents AS
       SELECT players.id AS player, m2.player AS opponent
       FROM players LEFT JOIN match_outcomes AS m1 ON players.id = m1.player
            JOIN match_outcomes AS m2 ON m1.match_id = m2.match_id AND m1.player != m2.player;

--Add an opponent match wins column to mwd_table, which counts the total number of wins
--of a player's opponents.
CREATE VIEW mwd_table_with_omw AS
       SELECT *,
       (SELECT sum(mwd2.wins)
                  FROM mwd_table AS mwd1 LEFT JOIN opponents ON mwd1.id = opponents.player
                       LEFT JOIN mwd_table AS mwd2 ON opponents.opponent = mwd2.id
                  WHERE mwd1.id = mwd_table.id
                  GROUP BY mwd1.id) AS omw
       FROM mwd_table;

--Add a ranking field that ranks players so that wins are worth 3 points, byes are worth 3 points
--and draws are worth 1 point. This view was inspired by the responses to the 
--following stackoverflow thread that I started:
--http://stackoverflow.com/questions/32766068/how-to-select-distinct-records-across-two-self-joined-columns
CREATE VIEW ranked_players AS
       SELECT *, rank() OVER (ORDER BY mwd_table_with_omw.wins*3 + mwd_table_with_omw.byes*3 + mwd_table_with_omw.draws,
                                       mwd_table_with_omw.omw,
                                       mwd_table_with_omw.id) AS player_rank)
       FROM mwd_table_with_omw;
