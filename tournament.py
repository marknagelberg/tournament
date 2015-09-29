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
    c.execute('''DELETE FROM match_outcomes;''')
    c.execute('''DELETE FROM matches;''')
    conn.commit()
    conn.close()


def deletePlayers():
    """Remove all the player records from the database."""
    conn = connect()
    c = conn.cursor()
    c.execute('''DELETE FROM players;''')
    conn.commit()
    conn.close()


def countPlayers():
    """Returns the number of players currently registered."""
    conn = connect()
    c = conn.cursor()
    c.execute('''SELECT count(*)
                 FROM players;''')
    number_of_players = c.fetchone()[0]
    conn.close()
    return number_of_players


def registerPlayer(name):
    """Adds a player to the tournament database.
  
    Args:
      name: the player's full name (need not be unique).
    """
    conn = connect()
    c = conn.cursor()
    c.execute('''INSERT INTO players (name, bye)
                 VALUES (%s, FALSE);''', (name,))
    conn.commit()
    conn.close()


def playerStandings():
    """Returns a list of the players and their win records, sorted by points
       determined by the formula 3*wins + 3*byes + draws. This scoring scheme
       is taken from http://www.wizards.com/dci/downloads/swiss_pairings.pdf

    Returns:
      A list of tuples, each of which contains (id, name, wins, matches):
        id: the player's unique id (assigned by the database)
        name: the player's full name (as registered)
        wins: the number of matches the player has won
        matches: the number of matches the player has played
    """
    conn = connect()
    c = conn.cursor()

    c.execute('''SELECT id,
                        name,
                        wins,
                        matches
                 FROM ranked_players;''')
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

    #Add a new match id m_id and retrieve it.
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
    player with an equal or nearly-equal score (3 * #wins + 3 * #byes + #draws).
    That is, a player is matched to the player adjacent in the standings in the
    ranked_players view.

    If there are an odd number of players, a bye is assigned to a player that has
    not received one yet and that player is not returned in the list of pairs.
  
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
        c.execute('''SELECT id
                     FROM players
                     WHERE bye = FALSE;''')
        bye_player = c.fetchone()[0]
        reportMatch(winner = bye_player)
        c.execute('''UPDATE players
                     SET bye = TRUE
                     WHERE id = %s;''', (bye_player,))
        conn.commit()

        #If all of the players have a bye, then reassign
        #everyone's bye to 0 to restart the process.
        c.execute('''SELECT *
                     FROM players
                     WHERE bye = FALSE;''')
        if not c.fetchall():
          c.execute('''UPDATE players
                       SET bye = FALSE;''')

    #Select pairs from the ranked_players view with the bye player removed.
    c.execute('''WITH ranked_removing_bye AS 
                (SELECT * FROM ranked_players WHERE ranked_players.id != %s) 
                SELECT t1.id,
                       t1.name,
                       t2.id,
                       t2.name
                 FROM ranked_removing_bye AS t1
                      JOIN ranked_removing_bye AS t2 ON t1.player_rank + 1 = t2.player_rank
                 WHERE mod(t1.player_rank, 2) = 1
                 AND mod(t2.player_rank, 2) = 0;''', (bye_player, ))
    pairs = c.fetchall()
    conn.close()
    return pairs


