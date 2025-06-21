import pandas as pd
from pymongo import MongoClient, errors
import os
import sys

# Ottieni il percorso assoluto della directory dello script
script_dir = os.path.dirname(os.path.abspath(__file__))

# MODIFICA: Percorso diretto alla cartella notebooks
notebooks_dir = os.path.join(script_dir, "../notebooks")

# Costruisci i percorsi assoluti per i file CSV
file_paths = {
    "videogames_2016": os.path.join(notebooks_dir, "videogames_2016"),
    "videogames_2024": os.path.join(notebooks_dir, "videogames_2024")
}


def insert_into_mongodb(collection_name, data, db):
    if data is None or len(data) == 0:
        print(f"Nessun dato da inserire per {collection_name}.")
        return

    try:
        collection = db[collection_name]
        collection.delete_many({})
        result = collection.insert_many(data)
        print(f"Inseriti {len(result.inserted_ids)} documenti in '{collection_name}'.")
    except errors.BulkWriteError as bwe:
        print(f"Errore di scrittura in blocco: {bwe.details}")
    except Exception as e:
        print(f"Errore durante l'inserimento in MongoDB: {e}")


def main():
    try:
        print(f"Cartella notebooks: {os.path.abspath(notebooks_dir)}")
        print(f"Contenuto cartella notebooks: {os.listdir(os.path.abspath(notebooks_dir))}")

        # Verifica esistenza file
        missing_files = []
        for name, path in file_paths.items():
            abs_path = os.path.abspath(path)
            if not os.path.exists(abs_path):
                missing_files.append((name, abs_path))
            else:
                print(f"File {name} trovato: {abs_path}")

        if missing_files:
            print("\nERRORE: File mancanti!")
            for name, path in missing_files:
                print(f"- {name}: {path}")
            print("\nAssicurati che:")
            print("1. I file esistano in:", os.path.abspath(notebooks_dir))
            print("2. I nomi corrispondano esattamente a:")
            print("   - videogames_2016.csv")
            print("   - videogames_2024.csv")
            sys.exit(1)

        with MongoClient("mongodb://localhost:27017/") as client:
            db = client["KeybladeDB"]

            # Caricamento dati
            df_2016 = pd.read_csv(file_paths["videogames_2016"])
            insert_into_mongodb("videogames_2016", df_2016.to_dict(orient='records'), db)

            df_2024 = pd.read_csv(file_paths["videogames_2024"])
            insert_into_mongodb("videogames_2024", df_2024.to_dict(orient='records'), db)

    except errors.ConnectionFailure as cf:
        print("Connessione a MongoDB fallita:", cf)
    except Exception as e:
        print(f"Errore imprevisto: {e}")


if __name__ == "__main__":
    main()