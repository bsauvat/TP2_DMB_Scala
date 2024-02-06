# TP2 - DMB

Réalisé par Quentin LEGRAND et Bastien SAUVAT

## 💻 1 Préparation des données

Les données que nous utilisons dans ce TP sont disponibles à l'adresse :
https://s3.amazonaws.com/tripdata/JC-202112-citibike-tripdata.csv.zip.

Le dataset est situé dans le répertoire `src/data` et est nommé `JC-202112-citibike-tripdata.csv`.

## 🧮 2 Calcul de degré

Voici le top 10 des stations les plus fréquentées par les trajets sortants et entrants :

| Classement | Station | Nombre de trajets sortants |
| --- | --- | --- |
| 1 | Grove St PATH | 1650 |
| 2 | Hoboken Terminal - River St & Hudson Pl | 1500 |
| 3 | Sip Ave | 1229 |
| 4 | Hoboken Terminal - Hudson St & Hudson Pl | 1136 |
| 5 | Hamilton Park | 929 |
| 6 | Newport PATH | 903 |
| 7 | City Hall - Washington St & 1 St | 864 |
| 8 | South Waterfront Walkway - Sinatra Dr & 1 St | 844 |
| 9 | Newport Pkwy | 819 |
| 10 | 8 St & Washington St | 747 |

| Classement | Station                                  | Nombre de trajets entrants |
| --- |------------------------------------------| --- |
| 1 | Grove St PATH                            | 1640 |
| 2 | Hoboken Terminal - River St & Hudson Pl  | 1408 |
| 3 | Hoboken Terminal - Hudson St & Hudson Pl | 1149 |
| 4 | Sip Ave                                  | 1094 |
| 5 | Hamilton Park                            | 954 |
| 6 | City Hall - Washington St & 1 St         | 908 |
| 7 | Newport PATH                             | 879 |
| 8 | South Waterfront Walkway - Sinatra Dr & 1 St | 835 |
| 9 | Newport Pkwy                             | 809 |
| 10 | Hoboken Ave at Monmouth St               | 790 |


## 📍 3 Proximité entre les stations

Voici les résultats obtenus :

La station la plus proche de JC013 en termes de distance est : **Hoboken Terminal - Hudson St & Hudson Pl, Distance: 0.10025826500541589 km**.

La station la plus proche de JC013 en termes de durée de trajet est : **Hoboken Terminal - River St & Hudson Pl**.

En effet, nous ne trouvons pas City Hall comme la plupart des étudiants de notre groupe.

Il se trouve un peu plus bas dans la liste des stations les plus proches :
Voici notre top 10 pour un meilleur aperçu :


| Classement | Station                                      | Distance |
| --- |----------------------------------------------| --- |
| 1 | Hoboken Terminal - Hudson St & Hudson Pl     | 0.10025826500541589 km |
| 2 | Hoboken Terminal - River St & Hudson Pl      | 0.10025826500541589 km |
| 3 | Brunswick & 6th                              | 0.13955090726842034 km |
| 4 | Monmouth and 6th                             | 0.13955090726842034 km |
| 5 | 6 St & Grand St                              | 0.15103965357911317 km |
| 6 | Clinton St & 7 St                            | 0.15103965357911317 km |
| 7 | South Waterfront Walkway - Sinatra Dr & 1 St | 0.1523123999966474 km |
| 8 | **City Hall - Washington St & 1 St**         | 0.16780879329552495 km |
| 9 | 7 St & Monroe St                             | 0.17011887657345162 km |
| 10 | 9 St HBLR - Jackson St & 8 St                | 0.17011887657345162 km |

| Classement | Station                                 | Durée |
| --- |-----------------------------------------| --- |
| 1 | Hoboken Terminal - River St & Hudson Pl | 0.0 secondes |
| 2 | 11 St & Washington St                   | 1000.0 secondes | 
| 3 | 8 St & Washington St                    | 1000.0 secondes |
| 4 | **City Hall - Washington St & 1 St**    | 1000.0 secondes |
| 5 | Liberty Light Rail                      | 2000.0 secondes |
| 6 | Manila & 1st                            | 2000.0 secondes |
| 7 | Paulus Hook                             | 2000.0 secondes |
| 8 | Dixon Mills                             | 2000.0 secondes |
| 9 | 9 St HBLR - Jackson St & 8 St           | 2000.0 secondes |
| 10 | Warren St                               | 2000.0 secondes |

