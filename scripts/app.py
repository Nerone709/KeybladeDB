from flask import Flask, render_template, request,Response
import os
import pymongo
import json
from pygments.lexer import combined

import queries

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
template_dir = os.path.join(BASE_DIR, "template")
static_dir = os.path.join(BASE_DIR, "static")

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
print('Connessione al database in corso…')
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['KeybladeDB']

videogames2016 = db['videogames_2016']
videogames2024 = db['videogames_2024']

print("Connesso. Avvio dell'app in corso…")
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/genere', methods=['GET', 'POST'])
def genre():
    genre_list = queries.get_all_genres(videogames2024)
    if genre_list is None:
        return Response("Errore: Impossibile recuperare la lista dei generi.", status=500)

    selected_genre = None
    combined_results = []
    if request.method == 'POST':
        selected_genre = request.form.get('selected_genre')
        results_2024 = queries.get_sales_by_genre(videogames2024, "2024", selected_genre)
        results_2016 = queries.get_sales_by_genre(videogames2016, "2016", selected_genre)

        seen = set()
        for game in results_2016 + results_2024:
            key = (game["title"].lower(), game["console"].lower())
            if key not in seen:
                seen.add(key)
                combined_results.append(game)
                combined_results.sort(key=lambda g: not bool(g.get("img")))

        return render_template('genere.html', risultati=combined_results, genre_list=genre_list, selected_genre=selected_genre)

    return render_template('genere.html', risultati=combined_results, genre_list=genre_list, selected_genre=selected_genre)

@app.route('/giochi')
def giochi():
    return "<h2>Lista dei giochi (in costruzione)</h2>"

@app.route('/contatti')
def contatti():
    return render_template('contatti.html')


@app.route('/rating', methods=['GET', 'POST'])
def rating_page():
    rating_list = queries.get_all_ratings(videogames2016)
    if rating_list is None:
        return Response(
            "Errore: Impossibile recuperare la lista delle valutazioni.",status=500)
    selected_rating = None
    results = []

    if request.method == 'POST':
        selected_rating = request.form.get('selected_rating')
        results = list(queries.get_videogames_by_rating(selected_rating, videogames2016))
        if results is None:
            return Response("Errore: Impossibile recuperare i risultati per la valutazione selezionata.",status=500)

    return render_template(
        'rating.html',
        rating_list=rating_list,
        selected_rating=selected_rating,
        results=results
    )


@app.route('/sviluppatore', methods=['GET', 'POST'])
def developer_page():
    results = []
    selected_dataset = None
    developer_name = None

    # Recupera sviluppatori dai due dataset
    devs_2016 = queries.get_all_developer2016(videogames2016)
    devs_2024 = queries.get_all_developer2024(videogames2024)

    if devs_2016 is None or devs_2024 is None:
        return Response("Errore: Impossibile recuperare la lista degli sviluppatori.", status=500)

    # Unisci rimuovendo duplicati
    combined_developers_list = sorted(set(devs_2016 + devs_2024))

    if request.method == 'POST':
        selected_dataset = request.form.get('selected_dataset')
        developer_name = request.form.getlist('developer_name')  # <-- qui prende sempre una lista

        if selected_dataset == '2016':
            if developer_name:
                results = list(queries.get_sales_by_developer2016(videogames2016, developer_name))
                if results is None:
                    return Response("Errore: Impossibile recuperare i risultati per lo sviluppatore selezionato.",
                                    status=500)

        elif selected_dataset == '2024':
            if developer_name:
                results = list(queries.get_sales_by_developer2024(videogames2024, developer_name))
                if results is None:
                    return Response("Errore: Impossibile recuperare i risultati per lo sviluppatore selezionato.",
                                    status=500)
    else:
        developer_name = []

    return render_template(
        'developer.html',
        selected_dataset=selected_dataset,
        developer_name=developer_name,  # ora è sempre lista
        results=results,
        developers_list=combined_developers_list  # la lista di tutti i dev da mostrare nel select
    )

from flask import request, render_template

@app.route('/pubblicazioni', methods=['GET', 'POST'])
def developer():
    if request.method == 'POST':
        publisher = request.form.get('publisher', '')
        start_year = int(request.form.get('anno_inizio', 1980))
        end_year = int(request.form.get('anno_fine', 2024))

        # Ottieni i risultati da entrambi i dataset
        result_2024 = queries.get_publisher_range(publisher, start_year, end_year, videogames2024, "2024")
        result_2016 = queries.get_publisher_range(publisher, start_year, end_year, videogames2016, "2016")

        # Unisci i risultati, eliminando i duplicati (su titolo + console)
        seen = set()
        combined_results = []
        for game in result_2024 + result_2016:
            key = (game["title"].lower(), game["console"].lower())
            if key not in seen:
                seen.add(key)
                combined_results.append(game)

        return render_template('pubblicazioni.html', risultati=combined_results, publisher=publisher)

    # Se GET, mostra solo il form (oppure reindirizza a homepage)
    return render_template('pubblicazioni.html', risultati=None)



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000,debug=True)
