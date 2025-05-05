from pymongo import MongoClient
import pandas as pd

# Percorso del file CSV (modifica se necessario)
file_path = "../dataset/videogames_sales.csv"

# Caricamento del dataset con Pandas
df = pd.read_csv(file_path)

# Pulizia del dataset
if 'Rating' in df.columns:
    df['Rating'] = df['Rating'].fillna('RP')

# Conversione della colonna User_Score in formato numerico
df['User_Score'] = pd.to_numeric(df['User_Score'], errors='coerce')

columns_to_update = ['Critic_Score', 'Critic_Count', 'User_Score', 'User_Count']
for column in columns_to_update:
    if column in df.columns:
        df[column] = df[column].fillna(0.0)

if 'Name' in df.columns:
    df = df.dropna(subset=['Name'])

# Sostituisci i valori NaN con "Unknown" nelle colonne Publisher e Developer
df['Publisher'] = df['Publisher'].fillna("Unknown")
df['Developer'] = df['Developer'].fillna("Unknown")


# Sostituisci i valori NaN nella colonna Year_of_Release con "Unknown"
df['Year_of_Release'] = df['Year_of_Release'].fillna("Unknown")

# Connessione a MongoDB locale
client = MongoClient("mongodb://localhost:27017/")
db = client["Keyblade"]  # Nome del database
collection = db["dati_csv"]  # Nome della collezione

# Conversione del dataframe in un dizionario e inserimento in MongoDB
dati = df.to_dict(orient='records')
if dati:
    result = collection.insert_many(dati)
    print(f"Inseriti {len(result.inserted_ids)} documenti nel database.")
else:
    print("Nessun dato trovato nel dataframe.")