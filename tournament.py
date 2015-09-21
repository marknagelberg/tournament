#!/usr/bin/env python
# 
# tournament.py -- implementation of a Swiss-system tournament
#

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
    """Returns a list of the players and their win records, sorted by wins.

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
    #Create a table as a view that contains player ids and all of the 
    #opponents that player has had. (Next step will be to joining with 
    #matches table and aggregating to get OMW.
    c.execute('''CREATE VIEW mwd_table AS
                 SELECT id,
                  (SELECT count(*)
                   FROM match_outcomes
                   WHERE match_outcomes.player = players.id) AS matches,
                  (SELECT count(*)
                   FROM match_outcomes
                   WHERE match_outcomes.player = players.id AND match_outcomes.player_outcome = 'W') AS wins,
                  (SELECT count(*)
                   FROM match_outcomes
                   WHERE match_outcomes.player = players.id AND match_outcomes.player_outcome = 'D') AS draws,
                  (SELECT count(m3.player)
                   FROM match_outcomes AS m1, match_outcomes AS m2, match_outcomes AS m3
                   WHERE m1.player = players.id AND m1.player != m2.player AND m1.match_id = m2.match_id AND m2.player = m3.player AND m3.player_outcome = 'W'
                   GROUP BY m1.player) AS omw
                 FROM players;''')
    c.execute('''SELECT players.id, name, wins, matches FROM players, mwd_table WHERE players.id = mwd_table.id ORDER BY (wins*3 + draws) DESC, omw DESC;''')
    standings = c.fetchall()
    conn.close()
    return standings


def reportMatch(winner, loser = None, draw = False):
    """Records the outcome of a single match between two players.

    Args:
      winner:  the id number of the player who won
      loser:  the id number of the player who lost
    """
    conn = connect()
    c = conn.cursor()
    #First make sure that the players have not played before (prevent rematches)
    c.execute('SELECT * from match_outcomes as m1, match_outcomes as m2 WHERE m1.match_id = m2.match_id AND m1.player != m2.player AND m1.player = %s AND m2.player = %s;', (winner, loser))
    if c.fetchone():
      print "Cannot add match - these players have played together already!"
      return

    c.execute('''INSERT INTO matches DEFAULT VALUES;''')
    c.execute('''SELECT max(id) FROM matches;''')
    m_id = c.fetchone()

    if not loser:
      c.execute('''INSERT INTO match_outcomes (match_id, player, player_outcome) VALUES (%s, %s, 'B');''', (m_id, winner))
    elif draw:
      c.execute('''INSERT INTO match_outcomes (match_id, player, player_outcome) VALUES (%s, %s, 'D');''', (m_id, winner))
      c.execute('''INSERT INTO match_outcomes (match_id, player, player_outcome) VALUES (%s, %s, 'D');''', (m_id, loser))
    else:
      c.execute('''INSERT INTO match_outcomes (match_id, player, player_outcome) VALUES (%s, %s, 'W');''', (m_id, winner))
      c.execute('''INSERT INTO match_outcomes (match_id, player, player_outcome) VALUES (%s, %s, 'L');''', (m_id, loser))
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

    #Takes the form [(player1, name1, ...), (player2, name2, ...), ...]
    ranked_players = playerStandings()

    #Must assign a bye if there are odd # players
    odd_num_players = len(ranked_players) % 2

    #Initialize array of pairs to return
    pairs = []

    #Assign a bye, add a win and bye to their record
    if odd_num_players:
        conn = connect()
        c = conn.cursor()
        c.execute('''SELECT id,
                          name,
                   FROM players
                   WHERE bye = FALSE;''')
        bye_player = c.fetchone()
        reportMatch(bye_player[0])
        c.execute('''UPDATE players
                   SET bye = TRUE
                   WHERE id = %s;''', (bye_player[0],))
        conn.commit()
        conn.close()
        ranked_players.remove(bye_player)

    #ranked_players must have even number of players.
    #Build swiss pairs.
    while ranked_players:
      player1 = ranked_players.pop(0)[:2]
      player2 = ranked_players.pop(0)[:2]
      pairs.append(player1 + player2)

    return pairs


