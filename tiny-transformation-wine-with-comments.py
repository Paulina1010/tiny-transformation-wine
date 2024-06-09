from collections import namedtuple
import csv
'''
funkcja konwertuje dane dla poszczególnych wartości
czyli funkcja bierze wiersz i wykonuje replace na wybranych elementach.
Tutaj robi replace na Rating, gdzie zmienia typ danych na float.
'''
def convert(row):
    return row._replace( #zwraca nową krotkę, która ma zmienione wybrane elementy
        Rating=float(row.Rating),
        NumberOfRatings=int(row.NumberOfRatings),
        Price=float(row.Price),
    )

#csvfile - identyfikator/uchwyt otwartego pliku

with open('white.csv', newline='') as csvfile: #wczytujemy linię z pliku csv
    reader = csv.reader(csvfile, delimiter=',') #tworzony jest iterator klasy csv.reader, który służy do rozdzielania kolumn przecinkami
    Row = namedtuple('Row', next(reader)) #'Row' (typename) - nazwa klasy dziedziczącej z krotki
    #print(Row) -> <class '__main__.Row'>
    '''
    Row - tworzona jest klasa o nazwie Row, ponieważ namedtuple zwraca klasę. Row to klasa specjalnej krotki (namedtuple). 
    (W krotce numedtuple, krotki mają swoje indeksy i do tych indeksów sa przyporządkowane nazwy kolumn.)
    Instancjami tej klasy są wiersze.
    W momencie wywołania next obiekt csv.reader używa uchwytu do pliku (id) i rozdziela pola przecinkiem i otrzymujemy krotki.
    
    Ustalamy nagłówek poprzez wczytanie danych do namedtuple
    
    map(fn, it): for x in it: yield fn(x)
    '''
    # for row in map(convert, map(Row._make, reader)):
    for row in reader:
        # row jest listą stringów
        #print(row) -> ['Vinho Verde Branco N.V.', 'Portugal', 'Vinho Verde', 'Casal Garcia', '3.7', '62980', '4.35', 'N.V.']
        row = Row._make(row)
        #print(row) -> Row(Name='Vinho Verde Branco N.V.', Country='Portugal', Region='Vinho Verde', Winery='Casal Garcia', Rating='3.7', NumberOfRatings='62980', Price='4.35', Year='N.V.')
        # row jest Rowem (krotka z nazwami kolumn)
        row = convert(row)
        # row jest Rowem (z poprawionymi typami)
        '''
        wynikiem mapa jest iterator
        funkcja _make należy do klasy Row
        for na map woła next, ten map woła next map wew
        Na Vermentino 2017,Italy,T wywołuje make i otrzymuje informacje o nazwie kolumny
        
        
        W pierwszej pętli for bierzemy wiersz z wartościami
        map bierze wartości spamreader i wrzuca je do make, make zwraca wiersz wraz z nagłówkiem
        następnie wykonywany jest drugi map, który wykonuje funkcję convert na elemencie

        Pętla for bierze poszczególne wartości z wierszy i wykonuje na nich funkcję zip.
        Funkcja zipuje Nagłówek kolumny z wartością.
        '''
        for x, colname in zip(row, Row._fields):
            print(colname, x, type(x).__name__, end=' | ') #wywołanie elementów wiersza wraz z typem danych i rozdzielenie elementów znakiem |
        print()
        print()
        #Name Vinho Verde Branco N.V. str | Country Portugal str | Region Vinho Verde str | Winery Casal Garcia str | Rating 3.7 float | NumberOfRatings 62980 int | Price 4.35 float | Year N.V. str |