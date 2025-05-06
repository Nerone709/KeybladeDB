from pymongo import MongoClient
import pandas as pd

# Percorsi dei file CSV
file_path_2016 = "../datasets/videogames_sales2016.csv"
file_path_2024 = "../datasets/videogames_sales2024.csv"

# Funzione per il caricamento e la pulizia del dataset videogames_sales2016.csv
def load_and_clean_csv_2016(file_path):
    df = pd.read_csv(file_path)

    # Cambiamento di Rating null in RP
    if 'Rating' in df.columns:
        df['Rating'] = df['Rating'].fillna('RP')

    # Conversione User_Score da stringa a numero
    df['User_Score'] = pd.to_numeric(df['User_Score'], errors='coerce')
    columns_to_update = ['Critic_Score', 'Critic_Count', 'User_Score', 'User_Count']

    # Sostituzione dei valori NaN con 0.0 per le colonne specificate
    for column in columns_to_update:
        if column in df.columns:
            df[column] = df[column].fillna(0.0)

    # Eliminazione dei giochi senza nome
    if 'Name' in df.columns:
        df = df.dropna(subset=['Name'])

    # Conversione dei valori NaN in Unknown
    df['Publisher'] = df['Publisher'].fillna("Unknown")
    df['Developer'] = df['Developer'].fillna("Unknown")
    df['Year_of_Release'] = df['Year_of_Release'].fillna("Unknown")

    return df

# Funzione per il caricamento e la pulizia del dataset videogames_sales2024.csv
def load_and_clean_csv_2024(file_path):
    df = pd.read_csv(file_path)

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('Unknown')
        else:
            df[col] = df[col].fillna(0.0)

    # Conversione dei valori Data di rilascio e last_update in formato data (YYYY)
    df.loc[df['release_date'] != 'Unknown', 'release_date'] = pd.to_datetime(
        df.loc[df['release_date'] != 'Unknown', 'release_date'], errors='coerce'
    ).dt.year.astype('Int64')

    # Conversione di last_update come la colonne release_date
    df.loc[df['last_update'] != 'Unknown', 'last_update'] = pd.to_datetime(
        df.loc[df['last_update'] != 'Unknown', 'last_update'], errors='coerce'
    ).dt.year.astype('Int64')

    df.drop('img', axis=1, inplace=True)
    return df

# Effettuo la pulizia dei due csv richiamando i metodi
df_2016 = load_and_clean_csv_2016(file_path_2016)
df_2024 = load_and_clean_csv_2024(file_path_2024)

# Connessione a MongoDB locale
client = MongoClient("mongodb://localhost:27017/")
db = client["Keyblade"]  # Nome del database

# Inserimento dei dati del dataset 2016
collection_2016 = db["videogames_2016"]
dati_2016 = df_2016.to_dict(orient='records')
if dati_2016:
    result_2016 = collection_2016.insert_many(dati_2016)
    print(f"Inseriti {len(result_2016.inserted_ids)} documenti nella collezione 'videogames_2016'.")

# Inserimento dei dati del dataset 2024
collection_2024 = db["videogames_2024"]
dati_2024 = df_2024.to_dict(orient='records')
if dati_2024:
    result_2024 = collection_2024.insert_many(dati_2024)
    print(f"Inseriti {len(result_2024.inserted_ids)} documenti nella collezione 'videogames_2024'.")