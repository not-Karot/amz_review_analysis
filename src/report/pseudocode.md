# Map Reduce pseudocode

### Job 1

#### Mapper
1. Per ogni riga nell'input letto da stdin:
   - Rimuovere gli spazi bianchi iniziali e finali dalla riga
   - Dividere la riga in product_id, time e text
   - Convertire il timestamp time in year
   - Rimuovere i tag HTML dal campo text 
   - Sostituire ogni punto in text con uno spazio bianco
   - Rimuovere la punteggiatura dal campo text
   - Rimuovere gli spazi bianchi multipli dal campo text
   - Estrarre le parole di almeno 4 caratteri dal campo text in minuscolo
   - Per ogni parola estratta:
     * Stampare in output (year, product_id) come chiave e (word, 1) come valore

#### Reducer

1. Creare un dizionario vuoto chiamato word_counts per memorizzare le parole e le recensioni per prodotto e anno e un altro dizionario vuoto chiamato review_counts per memorizzare solo il conteggio delle recensioni per prodotto e anno.
2. Leggere i dati dal mapper tramite la funzione sys.stdin:
   - Rimuovere gli spazi bianchi iniziali e finali dalla riga.
   - Dividere la riga in year, product_id, word e count, e convertire count in un numero intero.
   - Aggiornare i dizionari word_counts e review_counts:
     * Incrementare il conteggio della parola per quel prodotto e anno nel dizionario word_counts.
     * Incrementare il conteggio delle recensioni per quel prodotto e anno nel dizionario review_counts.
3. Trovare i 10 prodotti più recensiti per ogni anno e memorizzarli in un dizionario chiamato top_products.
4. Calcolare e stampare la frequenza delle parole per i 10 prodotti più recensiti per ogni anno:
   - Per ogni anno nel dizionario word_counts, per ogni prodotto e conteggio di parole nel dizionario word_counts[year]:
   * Se il prodotto fa parte dei 10 prodotti più recensiti per quell'anno, stampare l'output nel formato (year-product_id, word, count).
### Job 2

#### Mapper
1. Per ogni riga nell'input letto da stdin:
   - Rimuovere gli spazi bianchi iniziali e finali dalla riga.
   - Dividere la riga in campi: user_id, helpfulness_numerator e helpfulness_denominator.
   - Calcolare l'apprezzamento (utility):
     * Se helpfulness_denominator non è uguale a 0:
       - Dividere helpfulness_numerator per helpfulness_denominator e assegnarlo a utility.
     * Altrimenti:
       - Impostare utility a 0.
   - Stampare user_id come chiave e utility come valore, separati da un carattere di tabulazione.

#### Reducer
1. Creare un dizionario vuoto chiamato user_data per memorizzare le somme delle utility e il conteggio delle recensioni per ogni utente.
2. Per ogni riga nell'input letto da stdin:
   - Rimuovere gli spazi bianchi iniziali e finali dalla riga.
   - Dividere la riga in user_id e utility, e convertire utility in un numero float.
   - Aggiornare il dizionario user_data:
     * Se user_id è già presente nel dizionario user_data:
       - Aggiungere utility alla somma delle utility per quell'utente e incrementare il conteggio delle recensioni.
     * Altrimenti:
       - Inserire una nuova voce nel dizionario con user_id come chiave e un dizionario con la somma delle utility e il conteggio delle recensioni come valore.
3. Calcolare la media delle utility per ogni utente nel dizionario user_data e creare un nuovo dizionario chiamato user_utility_avg.
4. Ordinare gli utenti in base all'apprezzamento (media delle utility) in ordine decrescente e memorizzarli in una lista chiamata sorted_users.
5. Stampare gli utenti ordinati e le loro medie di utility.

### Job 3

#### Mapper
#### Reducer