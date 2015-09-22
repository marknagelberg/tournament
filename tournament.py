#!/usr/bin/env python
# 
# tournament.py -- implementation of a Swiss-system tournament
# This implementation influenced by the directions provided 
# by DCI http://www.wizards.com/dci/downloads/swiss_pairings.pdf

import psycopg2


def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=tournament")


def deleteMatches():
    """Remove all the match records from the database."""
    conn = connect()
    c = conn.cursor()
    c.execute('DELETE from match_outcomes;')
    c.execute('DELETE from matches;')
    conn.commit()
    conn.close()


def deletePlayers():
    """Remove all the player records from the database."""
    conn = connect()
    c = conn.cursor()
    c.execute('DELETE from players;')
    conn.commit()
    conn.close()


def countPlayers():
    """Returns the number of players currently registered."""
    conn = connect()
    c = conn.cursor()
    c.execute('SELECT count(*) from players;')
    return c.fetchone()[0]


def registerPlayer(name):
    """Adds a player to the tournament database.
  
    The database assigns a unique serial id number for the player.  (This
    should be handled by your SQL database schema, not in your Python code.)
  
    Args:
      name: the player's full name (need not be unique).
    """
    conn = connect()
    c = conn.cursor()
    c.execute('INSERT INTO players (name, bye) values (%s, FALSE);', (name,))
    conn.commit()
    conn.close()


def playerStandings():
    """Returns a list of the players and their win records, sorted by points
       determined by the formula 3*wins + 3*byes + draws. This scoring scheme
       is taken from http://www.wizards.com/dci/downloads/swiss_pairings.pdf

    The first entry in the list should be the player in first place, or a player
    tied for first place if there is currently a tie.

    Returns:
      A list of tuples, each of which contains (id, name, wins, matches):
        id: the player's unique id (assigned by the database)
        name: the player's full name (as registered)
        wins: the number of matches the player has won
        matches: the number of matches the player has played
    """
    conn = connect()
    c = conn.cursor()

    #Create table consisting of player, matches, wins, draws, and
    #opponent match wins (omw)
    c.execute('''CREATE VIEW mwd_table AS
                 SELECT id,
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
                 FROM players;''')

    #Rank the players (wins worth 3, draws worth 1, byes worth 3)
    c.execute('''SELECT players.id,
                        name,
                        wins,
                        matches
                 FROM players,
                      mwd_table
                 WHERE players.id = mwd_table.id
                 ORDER BY (wins*3 + draws + byes*3) DESC,
                          omw DESC;''')

    standings = c.fetchall()
    conn.close()
    return standings


def reportMatch(winner, loser = None, draw = False):
    """Records the outcome of a single match between two players (or one player for byes).

    Args:
      winner:  the id number of the player who won or received a bye
      loser:  the id number of the player who lost. If None, then winner is a bye.
      draw = boolean of whether match resulted in draw.
    """

    if loser == None and draw == True:
        raise ValueError("If loser is not specified, the match cannot be a draw (must be a bye).")

    conn = connect()
    c = conn.cursor()

    #First make sure that the players have not played before (prevent rematches)
    c.execute('''SELECT *
                 FROM match_outcomes as m1,
                      match_outcomes as m2
                 WHERE m1.match_id = m2.match_id
                       AND m1.player != m2.player
                       AND m1.player = %s
                       AND m2.player = %s;''', (winner, loser))
    if c.fetchone():
      raise ValueError("Cannot add match - these players have played together already!")
      return

    c.execute('''INSERT INTO matches DEFAULT VALUES;''')
    c.execute('''SELECT max(id) FROM matches;''')
    m_id = c.fetchone()

    if not loser:
      #Then count player as a bye
      c.execute('''INSERT INTO match_outcomes
                              (match_id,
                               player,
                               player_outcome)
                   VALUES (%s, %s, 'B');''', (m_id, winner))
    elif draw:
      #Assign each player a draw
      c.execute('''INSERT INTO match_outcomes
                              (match_id,
                               player,
                               player_outcome)
                   VALUES (%s, %s, 'D');''', (m_id, winner))
      c.execute('''INSERT INTO match_outcomes
                              (match_id,
                               player,
                               player_outcome)
                   VALUES (%s, %s, 'D');''', (m_id, loser))
    else:
      #Assign winner and loser matches accordingly
      c.execute('''INSERT INTO match_outcomes
                              (match_id,
                               player,
                               player_outcome)
                   VALUES (%s, %s, 'W');''', (m_id, winner))
      c.execute('''INSERT INTO match_outcomes
                              (match_id,
                               player,
                               player_outcome)
                   VALUES (%s, %s, 'L');''', (m_id, loser))
    conn.commit()
    conn.close()
 
 
def swissPairings():
    """Returns a list of pairs of players for the next round of a match.
  
    Assuming that there are an even number of players registered, each player
    appears exactly once in the pairings.  Each player is paired with another
    player with an equal or nearly-equal win record, that is, a player adjacent
    to him or her in the standings.
  
    Returns:
      A list of tuples, each of which contains (id1, name1, id2, name2)
        id1: the first player's unique id
        name1: the first player's name
        id2: the second player's unique id
        name2: the second player's name
    """
    conn = connect()
    c = conn.cursor()

    #Assign a bye if there are odd # players
    odd_num_players = countPlayers() % 2
    bye_player = -1
    if odd_num_players:
        c.execute('''SELECT id,
                            name
                     FROM players
                     WHERE bye = FALSE;''')
        bye_player = c.fetchone()[0]
        reportMatch(winner = bye_player)
        c.execute('''UPDATE players
                     SET bye = TRUE
                     WHERE id = %s;''', (bye_player,))
        conn.commit()

    #Create table consisting of player, matches, wins, draws, and
    #opponent match wins (omw)
    c.execute('''CREATE VIEW mwd_table AS
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
                 FROM players;''')

    #Find each possible pair of players and order pairs
    #so that they are adjacent to their closest scoring
    #competitors.
    c.execute('''CREATE VIEW before_duplicate_removal
                 AS
                  SELECT p1.id as player1,
                         p1.name as name1,
                         p2.id as player2,
                         p2.name AS name2
                  FROM mwd_table AS p1,
                       mwd_table AS p2
                  WHERE p1.id < p2.id
                        AND p1.id != %s
                        AND p2.id != %s
                  ORDER BY ABS(p1.wins*3 + p1.draws + p1.byes*3 - p2.wins*3 - p2.draws - p2.byes*3),
                           ABS(p1.omw - p2.omw);''', (bye_player, bye_player))

    #Ensure the same player does not appear in multiple rows and
    #prevent rematches.
    c.execute('''SELECT DISTINCT ON (player1 IN (SELECT player2 FROM before_duplicate_removal))
                                     player1,
                                     name1,
                                     player2,
                                     name2
                 FROM before_duplicate_removal
                 WHERE NOT EXISTS(SELECT * FROM match_outcomes as m1, match_outcomes AS m2
                                 WHERE m1.match_id = m2.match_id
                                 AND m1.player = player1
                                 AND m2.player = player2);''')
    pairs = c.fetchall()

    return pairs


