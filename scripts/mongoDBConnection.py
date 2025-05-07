import pandas as pd
from pymongo import MongoClient, errors

# These are the paths for the datasets
file_paths = {
    "videogames_2016": "../datasets/videogames_sales2016.csv",
    "videogames_2024": "../datasets/videogames_sales2024.csv"
}

def fill_missing_values(df, default_str="Unknown", default_num=0.0):
    """Substitute NaN values with default ones (Unknown for string types, 0.0 for number one)s"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna(default_str)
        else:
            df[col] = df[col].fillna(default_num)
    return df

# Function to load and clean the 2016 dataset
def load_and_clean_csv_2016(file_path):
    df = pd.read_csv(file_path)

    df['Rating'] = df.get('Rating', pd.Series()).fillna('RP')
    df['User_Score'] = pd.to_numeric(df.get('User_Score', pd.Series()), errors='coerce')

    # Valori numerici da sostituire con 0.0 se NaN
    for col in ['Critic_Score', 'Critic_Count', 'User_Score', 'User_Count']:
        df[col] = df.get(col, pd.Series()).fillna(0.0)

    df = df.dropna(subset=['Name'])
    df = fill_missing_values(df)
    return df

# Function to load and clean the 2024 dataset
def load_and_clean_csv_2024(file_path):
    df = pd.read_csv(file_path)
    df = fill_missing_values(df)

    for date_col in ['release_date', 'last_update']:
        if date_col in df.columns:
            mask = df[date_col] != 'Unknown'
            df.loc[mask, date_col] = pd.to_datetime(df.loc[mask, date_col], errors='coerce').dt.year.astype('Int64')

    df.drop(columns=['img'], errors='ignore', inplace=True)
    return df

# Function to insert data into MongoDB
def insert_into_mongodb(collection_name, data, db):
    """Insert datas into mongodb (with error handling)."""
    if not data:
        print(f"Nessun dato da inserire per {collection_name}.")
        return

    try:
        collection = db[collection_name]
        result = collection.insert_many(data)
        print(f"Inseriti {len(result.inserted_ids)} documenti nella collezione '{collection_name}'.")
    except errors.BulkWriteError as bwe:
        print(f"Errore di scrittura in blocco: {bwe.details}")
    except Exception as e:
        print(f"Errore durante l'inserimento in MongoDB: {e}")

def main():
    try:
        with MongoClient("mongodb://localhost:27017/") as client:
            db = client["Keyblade"]

            # Dataset 2016
            df_2016 = load_and_clean_csv_2016(file_paths["videogames_2016"])
            insert_into_mongodb("videogames_2016", df_2016.to_dict(orient='records'), db)

            # Dataset 2024
            df_2024 = load_and_clean_csv_2024(file_paths["videogames_2024"])
            insert_into_mongodb("videogames_2024", df_2024.to_dict(orient='records'), db)

    except errors.ConnectionFailure as cf:
        print("Connessione a MongoDB fallita:", cf)

if __name__ == "__main__":
    main()
