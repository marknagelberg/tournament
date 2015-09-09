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
    c.execute('INSERT INTO players (name, wins, matches) values (%s, 0, 0);', (name,))
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
    c.execute('SELECT id, name, wins, matches FROM players ORDER BY wins DESC;')
    return c.fetchall()


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
    conn = connect()
    c = conn.cursor()
    #NEED TO ADD A REQUIREMENT TO QUERY SO THAT IT AVOIDS REMATCHES
    #YOU ALREADY PREVENT THEM FROM BEING ADDED BUT DON'T WANT TO SUGGEST
    #THESE MATCHES
    c.execute('SELECT DISTINCT ON (p1.id) p1.id, p1.name, p2.id, p2.name FROM players AS p1, players AS p2 WHERE p1.wins = p2.wins AND p1.id < p2.id;')
    return c.fetchall()


