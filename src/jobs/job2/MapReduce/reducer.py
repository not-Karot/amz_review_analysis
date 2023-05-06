#!/usr/bin/env python
"""reducer.py"""

import sys

# Dizionario per memorizzare le somme delle utility e il conteggio delle recensioni per ogni utente
user_data = {}

# Leggi l'input riga per riga
for line in sys.stdin:
    line = line.strip()

    # Estrai user_id e utility dall'input
    user_id, utility = line.split('\t')
    utility = float(utility)

    # Aggiorna il dizionario user_data
    if user_id in user_data:
        user_data[user_id]['utility_sum'] += utility
        user_data[user_id]['count'] += 1
    else:
        user_data[user_id] = {'utility_sum': utility, 'count': 1}

# Calcola la media dell'utilità per ogni utente e crea un nuovo dizionario con le medie
user_utility_avg = {user_id: data['utility_sum'] / data['count'] for user_id, data in user_data.items()}

# Ordina gli utenti in base all'apprezzamento (media delle utility) in ordine decrescente
sorted_users = sorted(user_utility_avg.items(), key=lambda x: x[1], reverse=True)

# Stampa gli utenti ordinati e le loro medie di utility
for user_id, avg_utility in sorted_users:
    print(f"{user_id}\t{avg_utility}")
