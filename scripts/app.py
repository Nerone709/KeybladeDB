from datetime import datetime
from bson import ObjectId, errors
from bson.errors import InvalidId
from flask import Flask, render_template, request, Response, jsonify, redirect, url_for
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

        return render_template('genere.html', risultati=combined_results, genre_list=genre_list,
                               selected_genre=selected_genre)

    return render_template('genere.html', risultati=combined_results, genre_list=genre_list,
                           selected_genre=selected_genre)


@app.route('/giochi', methods=['GET', 'POST'])
def mostra_tutti_i_giochi():
    if request.method == 'POST':
        # Gestione cancellazione
        data = request.get_json()
        id = data.get("id")
        collezione = data.get("collezione")

        if str(collezione) == "2016":
            collection = videogames2016
        elif str(collezione) == "2024":
            collection = videogames2024
        else:
            return jsonify({"success": False, "error": "Collezione non valida"}), 400

        try:
            object_id = ObjectId(id)
        except:
            return jsonify({"success": False, "error": "ID non valido"}), 400

        success = queries.delete_game(collection, object_id)
        return jsonify({"success": success})

    # Metodo GET: carica la pagina
    giochi = queries.get_all_games(videogames2016, videogames2024)
    return render_template("giochi.html", giochi=giochi)


@app.route('/aggiungi', methods=['GET', 'POST'])
def aggiungi_gioco():
    if request.method == 'POST':
        titolo = request.form.get('titolo')
        console = request.form.get('console')
        genere = request.form.get('genere')
        publisher = request.form.get('publisher')
        developer = request.form.get('developer')
        anno = int(request.form.get('anno'))
        collezione = request.form.get('collezione')

        # Vendite
        na_sales = float(request.form.get('na_sales', 0))
        pal_sales = float(request.form.get('pal_sales', 0))
        jp_sales = float(request.form.get('jp_sales', 0))
        other_sales = float(request.form.get('other_sales', 0))
        critic_score = float(request.form.get('critic_score') or 0)

        global_sales = na_sales + pal_sales + jp_sales + other_sales

        # Prepara i dati in base alla collezione
        if collezione == "2016":
            game_data = {
                "Name": titolo,
                "Platform": console,
                "Genre": genere,
                "Publisher": publisher,
                "Developer": developer,
                "Year": anno,
                "NA_Sales": na_sales,
                "EU_Sales": pal_sales,
                "JP_Sales": jp_sales,
                "Other_Sales": other_sales,
                "Global_Sales": global_sales,
                "Critic_Score": critic_score
            }
            collection = videogames2016
        elif collezione == "2024":
            game_data = {
                "title": titolo,
                "console": console,
                "genre": genere,
                "publisher": publisher,
                "developer": developer,
                "year": anno,
                "na_sales": na_sales,
                "pal_sales": pal_sales,
                "jp_sales": jp_sales,
                "other_sales": other_sales,
                "global_sales": global_sales,
                "critic_score": critic_score
            }
            collection = videogames2024
        else:
            return "Collezione non valida", 400

        queries.add_game(collection, game_data)
        return redirect(url_for('mostra_tutti_i_giochi'))

    return render_template("aggiungi.html")


@app.route('/modifica/<id>', methods=['GET', 'POST'])
def modifica_gioco(id):
    gioco_id = id
    if request.method == 'POST':
        collezione = request.form.get('collezione')
        sales_type = request.form.get('sales_type')  # sales element chosen
        sales_value = request.form.get('sales_value')  # value for the sales element
        critic_score = request.form.get('critic_score')

        # 2016 e 2024 has different field names
        campi_2016 = {
            "critic_score": "Critic_Score",
            "NA_Sales": "NA_Sales",
            "EU_Sales": "EU_Sales",
            "JP_Sales": "JP_Sales",
            "Other_Sales": "Other_Sales",
        }

        campi_2024 = {
            "critic_score": "critic_score",
            "na_sales": "na_sales",
            "pal_sales": "pal_sales",
            "jp_sales": "jp_sales",
            "other_sales": "other_sales",
        }

        if collezione == "2016":
            collection = videogames2016
            mapping = campi_2016
        elif collezione == "2024":
            collection = videogames2024
            mapping = campi_2024
        else:
            return "Collezione non valida", 400

        update_data = {}

        # Added critic_score if present
        if critic_score:
            try:
                update_data[mapping["critic_score"]] = float(critic_score)
            except ValueError:
                pass

        # Added the select sales_type and sales_value
        if sales_type and sales_value:
            # Mapping sales_type
            if sales_type in mapping:
                try:
                    update_data[mapping[sales_type]] = float(sales_value)
                except ValueError:
                    pass

        if update_data:
            result = collection.update_one({"_id": gioco_id}, {"$set": update_data})
            if result.modified_count > 0:
                return redirect(url_for('mostra_tutti_i_giochi'))
            else:
                gioco = collection.find_one({"_id": gioco_id})
                error_msg = "Nessuna modifica effettuata o dati non validi."
                return render_template("modifica.html", gioco=gioco, collezione=collezione, error=error_msg, sales_type=sales_type)
        else:
            gioco = collection.find_one({"_id": gioco_id})
            error_msg = "Nessun dato inserito per la modifica."
            return render_template("modifica.html", gioco=gioco, collezione=collezione, error=error_msg, sales_type=sales_type)

    else:
        collezione = request.args.get('collezione')
        if collezione == "2016":
            collection = videogames2016
        elif collezione == "2024":
            collection = videogames2024
        else:
            return "Collezione non valida", 400

        gioco = collection.find_one({"_id": gioco_id})
        if not gioco:
            return "Gioco non trovato", 404

        # Sales type default value
        default_sales_type = None
        if collezione == "2016":
            default_sales_type = "NA_Sales"
        elif collezione == "2024":
            default_sales_type = "na_sales"

        return render_template("modifica.html", gioco=gioco, collezione=collezione, sales_type=default_sales_type)


@app.route('/contatti')
def contatti():
    return render_template('contatti.html')


@app.route('/rating', methods=['GET', 'POST'])
def rating_page():
    rating_list = queries.get_all_ratings(videogames2016)
    if rating_list is None:
        return Response(
            "Errore: Impossibile recuperare la lista delle valutazioni.", status=500)
    selected_rating = None
    results = []

    if request.method == 'POST':
        selected_rating = request.form.get('selected_rating')
        results = list(queries.get_videogames_by_rating(selected_rating, videogames2016))
        if results is None:
            return Response("Errore: Impossibile recuperare i risultati per la valutazione selezionata.", status=500)

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


@app.route('/vendite/gioco', methods=['GET', 'POST'])
def vendita():
    result = None
    title_submitted = None
    sales_labels = []
    sales_values = []

    # Recupera la lista unica di titoli da entrambi i dataset
    all_titles = sorted(set(
        db.videogames_2024.distinct("title") +
        db.videogames_2016.distinct("Name")
    ))

    if request.method == 'POST':
        game_title = request.form.get('title')
        year = request.form.get('year')
        title_submitted = game_title

        if game_title and year:
            collection = db[f"videogames_{year}"]
            result = queries.analyze_sales_by_region(game_title, year, collection)
            if result:
                sales_labels = list(result['sales_by_region'].keys())
                sales_values = list(result['sales_by_region'].values())

    return render_template("vendita_gioco.html",
                           result=result,
                           title_submitted=title_submitted,
                           sales_labels=sales_labels,
                           sales_values=sales_values,
                           all_titles=all_titles)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
