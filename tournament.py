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
    c.execute('INSERT INTO players (name, wins, draws, matches, bye) values (%s, 0, 0, 0, FALSE);', (name,))
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
    c.execute('CREATE VIEW playermatches AS SELECT players.id AS pid, CASE WHEN winner = players.id THEN loser WHEN loser = players.id THEN winner END AS opponent FROM players, matches WHERE players.id = winner OR players.id = loser')
    c.execute('CREATE VIEW omwtable AS SELECT id, COUNT(opponent) AS omw FROM playermatches, matches WHERE playermatches.opponent = winner GROUP BY id;')
    c.execute('SELECT players.id, name, wins, matches FROM players LEFT OUTER JOIN omwtable ON (players.id = omwtable.id) ORDER BY (wins*3 + draws) DESC, omwtable.omw DESC;')
    standings = c.fetchall()
    conn.close()
    return standings


def reportMatch(winner, loser):
    """Records the outcome of a single match between two players.

    Args:
      winner:  the id number of the player who won
      loser:  the id number of the player who lost
    """
    conn = connect()
    c = conn.cursor()
    #First make sure that the players have not played before (prevent rematches)
    c.execute('SELECT * from matches where winner = %s AND loser = %s OR winner = %s AND loser = %s', (winner, loser, loser, winner))
    if c.fetchone():
      print "Cannot add match - these players have played together already!"
      return

    c.execute('INSERT INTO matches (winner, loser) values (%s, %s);', (winner, loser))
    c.execute('UPDATE players SET matches = matches + 1 WHERE id = %s OR id = %s;', (winner, loser))
    c.execute('UPDATE players SET wins = wins + 1 WHERE id = %s;', (winner,))
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
                          wins,
                          matches
                   FROM players
                   WHERE bye = FALSE;''')
        bye_player = c.fetchone()
        c.execute('''UPDATE players
                     SET wins = wins + 1
                     WHERE id = %s;''', (bye_player[0],))
        c.execute('''UPDATE players
                   SET bye = TRUE
                   WHERE id = %s;''', (bye_player[0],))
        conn.commit()
        ranked_players.remove(bye_player)
        conn.close()

    #ranked_players must have even number of players.
    #Build swiss pairs.
    while ranked_players:
      player1 = ranked_players.pop(0)[:2]
      player2 = ranked_players.pop(0)[:2]
      pairs.append(player1 + player2)

    return pairs


