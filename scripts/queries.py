from re import match
from typing import Collection
import re



def get_videogames_by_rating(rating, collection: Collection):
    """"
    Retrieve video games from the database based on their rating.
    :param rating: The rating to filter video games by (e.g., 'E', 'T', 'M')
    :param collection: The MongoDB collection to query.
    :return: A DataFrame containing video games with the specified rating.
    """
    pipeline = [
        {
            "$match": {
                "Rating": rating
            }
        },
        {
            "$project": {
                "Name": 1,
                "Platform": 1,
                "Year_of_Release": 1,
                "Genre": 1,
                "Publisher": 1,
                "Global_Sales": 1,
                "_id": 0
            }
        }
    ]
    return collection.aggregate(pipeline)


def get_sales_by_developer2024(collection: Collection, developers):
    """"
    Retrieve total global sales for a specific developer.
    :param collection: The MongoDB collection for the year 2024.
    :param developers: List of developers to filter by (e.g. 'Nintendo').
    :return: A list of dictionaries containing the name, developer, platform, and global sales of the video games.
    """
    if isinstance(developers, str):
        developers = [developers]
    pipeline = [
        {"$match": {"developer": {"$in": developers}}},
        {"$project": {
            "_id": 0,
            "title": 1,
            "developer": 1,
            "console": 1,
            "total_sales": 1
        }},
        {"$sort": {"total_sales": -1}},  # Sort by Global_Sales in descending order
    ]

    return collection.aggregate(pipeline)


def get_sales_by_developer2016(collection: Collection, developers):
    """"
    Retrieve total global sales for a specific developer.
    :param collection: The MongoDB collection for the year 2016.
    :param developers: List of developers to filter by (e.g. 'Nintendo').
    :return: A list of dictionaries containing the name, developer, platform, and global sales of the video games.
    """
    if isinstance(developers, str):
        developers = [developers]
    pipeline = [
        {"$match": {"Developer": {"$in": developers}}},
        {"$project": {
            "_id": 0,
            "Name": 1,
            "Developer": 1,
            "Platform": 1,
            "Global_Sales": 1
        }},
        {"$sort": {"Global_Sales": -1}},  # Sort by Global_Sales in descending order
    ]

    return collection.aggregate(pipeline)


def get_sales_by_genre(collection: Collection, dataset_name: str, genre):
    """"
    Retrieve total global sales for a specific genre.
    :param collection: The MongoDB collection.
    :param dataset_name: The name of the dataset (e.g. '2024', '2016').
    :param genre: The genre to filter by (e.g. 'Action', 'Adventure').
    :return: A list of dictionaries containing the name, genre, platform, and global sales of the video games.
    """

    field_map = {
        "2024": {
            "title": "title",
            "console": "console",
            "release_date": "release_date",
            "genre": "genre",
            "publisher": "publisher",
            "img": "img",
            "total_sales": "total_sales"
        },
        "2016": {
            "title": "Name",
            "console": "Platform",
            "release_date": "Year_of_Realese",
            "genre": "Genre",
            "publisher": "Publisher",
            "img": "img",
            "total_sales": "Global_Sales"
        }
    }

    fields = field_map[dataset_name]

    pipeline = [
        {"$match": {fields["genre"]: genre}},
        {"$project": {
            "_id": 0,
            "title": f"${fields['title']}",
            "img": f"${fields['img']}",
            "console": f"${fields['console']}",
            "release_date": f"${fields['release_date']}",
            "genre": f"${fields['genre']}",
            "publisher": f"${fields['publisher']}",
            "total_sales": f"${fields['total_sales']}"
        }},
        {"$sort": {
            "total_sales": -1
        }}  # Sort by Global_Sales in descending order
    ]

    result = list(collection.aggregate(pipeline))
    return result


def get_num_videogames_by_publisher(collection: Collection, publisher, year):
    """
    Retrieve the video games released by a specific publisher.
    :param collection: The MongoDB collection to query.
    :param year: The range of years to filter video games by (e.g., 2020-2024).
    :param publisher: The publisher to filter video games by (e.g., 'Nintendo').
    :return: The video games released by the specified publisher.
    """
    pipeline = [
        {
            "$match": {
                "publisher": {
                    "$regex": publisher,  # Match publisher name or part of it
                    "$options": "i"  # Case-insensitive match
                },
                "release_date": {
                    "$gte": year
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "title": 1,
                "console": 1,
                "release_date": 1,
                "genre": 1,
                "publisher": 1,
                "total_sales": 1
            }
        }
    ]
    return collection.aggregate(pipeline)


def get_publisher_range(publisher_name, start_year, end_year, dataset_version: Collection, dataset_name: str):
    field_map = {
        "2024": {
            "title": "title",
            "console": "console",
            "release_date": "release_date",
            "genre": "genre",
            "publisher": "publisher",
            "total_sales": "total_sales"
        },
        "2016": {
            "title": "Name",
            "console": "Platform",
            "release_date": "Year_of_Realese",
            "genre": "Genre",
            "publisher": "Publisher",
            "total_sales": "Global_Sales"
        }
    }

    fields = field_map[dataset_name]

    pipeline = [
        {
            "$match": {
                fields["publisher"]: {
                    "$regex": publisher_name,
                    "$options": "i"
                },
                fields["release_date"]: {
                    "$gte": start_year,
                    "$lte": end_year
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "title": f"${fields['title']}",
                "console": f"${fields['console']}",
                "release_date": f"${fields['release_date']}",
                "genre": f"${fields['genre']}",
                "publisher": f"${fields['publisher']}",
                "total_sales": f"${fields['total_sales']}"
            }
        },
        {
            "$sort": {
                "release_date": 1
            }
        }
    ]

    return list(dataset_version.aggregate(pipeline))


def get_max_avg_critic_score_for_publisher2016(collection: Collection, publisher_name):
    pipeline = [
        {
            "$match": {
                "Critic_Score": {"$ne": None},
                "Publisher": publisher_name
            }
        },
        {
            "$addFields": {
                "Critic_Score_Norm": {"$divide": ["$Critic_Score", 10]}
            }
        },
        {
            "$group": {
                "_id": "$Publisher",
                "avg_critic_score": {"$avg": "$Critic_Score_Norm"}
            }
        },
        {
            "$sort": {"avg_critic_score": -1}
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "_id": 0,
                "Publisher": "$_id",
                "avg_critic_score": 1
            }
        }
    ]

    result = collection.aggregate(pipeline)

    if result:
        return result[0]  # ritorna solo il primo risultato
    else:
        return None


def get_max_avg_critic_score_for_publisher_2024(collection: Collection, publisher_name):
    pipeline = [
        {
            "$match": {
                "critic_score": {"$ne": None},
                "publisher": publisher_name
            }
        },
        {
            "$group": {
                "_id": "$publisher",
                "avg_critic_score": {"$avg": "$critic_score"}
            }
        },
        {
            "$sort": {"avg_critic_score": -1}
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "_id": 0,
                "publisher": "$_id",
                "avg_critic_score": 1
            }
        }
    ]

    result = collection.aggregate(pipeline)

    if result:
        return result[0]  # ritorna solo il primo risultato
    else:
        return None


def get_avg_critic_score_for_specific_developer_2016(collection: Collection, developer_name):
    pipeline = [
        {
            "$match": {
                "Critic_Score": {"$ne": None},
                "Developer": developer_name
            }
        },
        {
            "$addFields": {
                "Critic_Score_norm": {"$divide": ["$Critic_Score", 10]}
            }
        },
        {
            "$group": {
                "_id": "$Developer",
                "avg_Critic_Score": {"$avg": "$Critic_Score_norm"}
            }
        },
        {
            "$sort": {"avg_Critic_Score": -1}
        },
        {
            "$project": {
                "_id": 0,
                "Developer": "$_id",
                "avg_Critic_Score": 1
            }
        }
    ]

    return collection.aggregate(pipeline)


def get_avg_critic_score_for_specific_developer_2024(collection: Collection, developer_name):
    pipeline = [
        {
            "$match": {
                "critic_score": {"$ne": None},
                "developer": developer_name
            }
        },
        {
            "$group": {
                "_id": "$developer",
                "avg_critic_score": {"$avg": "$critic_score"}
            }
        },
        {
            "$sort": {"avg_critic_score": -1}
        },
        {
            "$project": {
                "_id": 0,
                "developer": "$_id",
                "avg_critic_score": 1
            }
        }
    ]

    return collection.aggregate(pipeline)


def get_avg_critic_score_for_specific_publisher_2016(collection: Collection, publisher_name):
    pipeline = [
        {
            "$match": {
                "Critic_Score": {"$ne": None},
                "Publisher": publisher_name
            }
        },
        {
            "$addFields": {
                "Critic_Score_norm": {"$divide": ["$Critic_Score", 10]}
            }
        },
        {
            "$group": {
                "_id": "$Publisher",
                "avg_Critic_Score": {"$avg": "$Critic_Score_norm"}
            }
        },
        {
            "$sort": {"avg_Critic_Score": -1}
        },
        {
            "$project": {
                "_id": 0,
                "Publisher": "$_id",
                "avg_Critic_Score": 1
            }
        }
    ]

    return collection.aggregate(pipeline)


def get_avg_critic_score_for_specific_publisher_2024(collection: Collection, publisher_name):
    pipeline = [
        {
            "$match": {
                "critic_score": {"$ne": None},
                "publisher": publisher_name
            }
        },
        {
            "$group": {
                "_id": "$publisher",
                "avg_critic_score": {"$avg": "$critic_score"}
            }
        },
        {
            "$sort": {"avg_critic_score": -1}
        },
        {
            "$project": {
                "_id": 0,
                "publisher": "$_id",
                "avg_critic_score": 1
            }
        }
    ]

    return collection.aggregate(pipeline)


def modify_game(collection, id, game_data):
    pipeline = {"_id": id}  # id stringa
    result = collection.update_one(pipeline, {"$set": game_data})

    if result.modified_count > 0:
        print(f"Game with ID {id} modified successfully.")
        return collection.find_one(pipeline)
    else:
        print(f"No game found with ID {id} or no changes made.")
        return None


#####################################################################

def analyze_sales_by_region(game_title, year, collection):
    field_map = {
        "2016": {
            "title": "Name",
            "na_sales": "NA_Sales",
            "jp_sales": "JP_Sales",
            "pal_sales": "EU_Sales",
            "other_sales": "Other_Sales"
        },
        "2024": {
            "title": "title",
            "na_sales": "na_sales",
            "jp_sales": "jp_sales",
            "pal_sales": "pal_sales",
            "other_sales": "other_sales"
        }
    }

    fields = field_map[str(year)]

    pipeline = [
        {
            "$match": {
                fields["title"]: {"$regex": f"^{game_title}$", "$options": "i"}
            }
        },
        {
            "$group": {
                "_id": f"${fields['title']}",
                "total_na_sales": {"$sum": f"${fields['na_sales']}"},
                "total_jp_sales": {"$sum": f"${fields['jp_sales']}"},
                "total_pal_sales": {"$sum": f"${fields['pal_sales']}"},
                "total_other_sales": {"$sum": f"${fields['other_sales']}"}
            }
        },
        {
            "$match": {
                "$or": [
                    {"total_na_sales": {"$gt": 0}},
                    {"total_jp_sales": {"$gt": 0}},
                    {"total_pal_sales": {"$gt": 0}},
                    {"total_other_sales": {"$gt": 0}}
                ]
            }
        }
    ]

    result = list(collection.aggregate(pipeline))

    if result:
        data = result[0]
        title = data["_id"]

        sales_by_region = {
            "Nord America": data["total_na_sales"],
            "Giappone": data["total_jp_sales"],
            "Europa/PAL": data["total_pal_sales"],
            "Altri": data["total_other_sales"]
        }

        max_region = max(sales_by_region, key=sales_by_region.get)
        min_region = min(sales_by_region, key=sales_by_region.get)

        return {
            "title": title,
            "year": year,
            "max_region": max_region,
            "max_value": sales_by_region[max_region],
            "min_region": min_region,
            "min_value": sales_by_region[min_region],
            "sales_by_region": sales_by_region
        }
    else:
        return None


def get_all_games(collection_2016, collection_2024,page=1,limit=100):
    field_map = {
        "2016": {
            "title": "Name",
            "console": "Platform",
            "developer": "Developer",
            "publisher": "Publisher",
            "na_sales": "NA_Sales",
            "jp_sales": "JP_Sales",
            "pal_sales": "EU_Sales",
            "other_sales": "Other_Sales"
        },
        "2024": {
            "title": "title",
            "console": "console",
            "developer": "developer",
            "publisher": "publisher",
            "na_sales": "na_sales",
            "jp_sales": "jp_sales",
            "pal_sales": "pal_sales",
            "other_sales": "other_sales"
        }
    }

    # Pipeline base per giochi 2024
    base_pipeline = [
        {
            "$project": {
                "_id": 1,
                "title": f"${field_map['2024']['title']}",
                "console": f"${field_map['2024']['console']}",
                "developer": f"${field_map['2024']['developer']}",
                "publisher": f"${field_map['2024']['publisher']}",
                "anno": {"$literal": 2024}
            }
        },
        {
            "$unionWith": {
                "coll": collection_2016.name,
                "pipeline": [
                    {
                        "$project": {
                            "_id": 1,
                            "title": f"${field_map['2016']['title']}",
                            "console": f"${field_map['2016']['console']}",
                            "developer": f"${field_map['2016']['developer']}",
                            "publisher": f"${field_map['2016']['publisher']}",
                            "anno": {"$literal": 2016}
                        }
                    }
                ]
            }
        },
        {"$skip": (page - 1) * limit},
        {"$limit": limit}
    ]

    risultati = list(collection_2024.aggregate(base_pipeline))
    return risultati


def search_games_by_title(collection_2016, collection_2024, search, page=1, limit=100):
    skip = (page - 1) * limit
    regex = re.compile(re.escape(search), re.IGNORECASE)

    giochi_2024 = list(collection_2024.find({"title": regex}).skip(skip).limit(limit))
    giochi_2016 = list(collection_2016.find({"Name": regex}).skip(skip).limit(limit))

    risultati = []
    for g in giochi_2024:
        risultati.append({
            "_id": str(g["_id"]),
            "title": g.get("title", "N/A"),
            "console": g.get("console", "N/A"),
            "developer": g.get("developer", "N/A"),
            "publisher": g.get("publisher", "N/A"),
            "anno": 2024
        })
    for g in giochi_2016:
        risultati.append({
            "_id": str(g["_id"]),
            "title": g.get("Name", "N/A"),
            "console": g.get("Platform", "N/A"),
            "developer": g.get("Developer", "N/A"),
            "publisher": g.get("Publisher", "N/A"),
            "anno": 2016
        })

    risultati.sort(key=lambda x: x["title"].lower())
    return risultati



def get_updated_game(collection: Collection):
    pipeline = [
        # Estrai l'anno dalla data di rilascio
        {
            "$addFields": {
                "release_year": {
                    "$toInt": {
                        "$substr": [{"$ifNull": ["$release_date", "0"]}, 0, 4]
                    }
                }
            }
        },
        # Filtra solo i giochi rilasciati dal 2017 in poi
        {
            "$match": {
                "release_year": {"$gte": 2017}
            }
        },
        # Proietta tutti i campi desiderati, incluso last_update
        {
            "$project": {
                "_id": 0,
                "title": 1,
                "console": 1,
                "developer": 1,
                "publisher": 1,
                "genre": 1,
                "na_sales": 1,
                "jp_sales": 1,
                "pal_sales": 1,
                "other_sales": 1,
                "total_sales": 1,
                "release_date": 1,
                "release_year": 1,
                "last_update": 1  # Include il campo last_update
            }
        },
        # Ordina per anno e poi per data di rilascio
        {
            "$sort": {
                "release_year": 1,
                "release_date": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))



def add_game(collection: Collection, game_data):
    """
    Aggiunge un nuovo gioco alla collezione MongoDB.
    :param collection: La collezione MongoDB in cui inserire il gioco.
    :param game_data: Un dizionario contenente i dati del gioco da inserire.
    :return: Il risultato dell'inserimento.
    """
    result = collection.insert_one(game_data)
    print(f"Gioco aggiunto con ID: {result.inserted_id}")
    return result


def delete_game(collection: Collection, id):
    pipeline = {"_id": id}

    result = collection.delete_one(pipeline)
    return result.deleted_count > 0


def get_all_title(collection_2016: Collection, collection_2024: Collection):
    titles_2016 = collection_2016.distinct("Name")
    titles_2024 = collection_2024.distinct("title")

    combined = set(titles_2016) | set(titles_2024)  # unisce e rimuove duplicati
    return sorted(combined)


def get_all_developer(collection_2016: Collection, collection_2024: Collection):
    titles_2016 = collection_2016.distinct("Developer")
    titles_2024 = collection_2024.distinct("developer")

    combined = set(titles_2016) | set(titles_2024)  # unisce e rimuove duplicati
    return sorted(combined)

def get_all_ratings(collection: Collection):
    """
    Retrieve the list of unique ratings from the MongoDB collection.
    :param collection: The MongoDB collection to query.
    :return: A sorted list of unique ratings.
    """
    return sorted(collection.distinct("Rating"))

def get_avg_user_score_by_developer(collection, developer_name):
    pipeline = [
        {
            "$match": {
                "User_Score": {"$ne": None},
                "Developer": {"$regex": developer_name, "$options": "i"}
            }
        },
        {
            "$group": {
                "_id": "$Developer",
                "avg_User_Score": {"$avg": "$User_Score"}
            }
        },
        {
            "$sort": {"avg_User_Score": -1}
        },
        {
            "$project": {
                "_id": 0,
                "Developer": "$_id",
                "avg_User_Score": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def get_avg_user_score_by_publisher(collection: Collection, publisher_name):
    pipeline = [
        {
            "$match": {
                "User_Score": {"$ne": None},
                "Publisher": {"$regex": publisher_name, "$options": "i"} # Use regex for partial matching and case-insensitivity
            }
        },
        {
            "$group": {
                "_id": "$Publisher",
                "avg_User_Score": {"$avg": "$User_Score"}
            }
        },
        {
            "$sort": {"avg_User_Score": -1}
        },
        {
            "$project": {
                "_id": 0,
                "Publisher": "$_id",
                "avg_User_Score": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def get_total_critic_count_by_developer_2016(collection: Collection):
    pipeline = [
        # 1. Filtra documenti con Critic_Count e Publisher non nulli
        {
            "$match": {
                "Critic_Count": {"$ne": None},
                "Developer": {"$ne": None}
            }
        },
        # 2. Raggruppa per Publisher e somma Critic_Count
        {
            "$group": {
                "_id": "$Developer",
                "total_Critic_Count": {"$sum": "$Critic_Count"}
            }
        },
        # 3. Ordina per somma decrescente
        {
            "$sort": {"total_Critic_Count": -1}
        },
        # 4. Proietta output pulito
        {
            "$project": {
                "_id": 0,
                "Developer": "$_id",
                "total_Critic_Count": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def get_total_critic_count_by_publisher_2016(collection: Collection):
    pipeline = [
        # 1. Filtra documenti con Critic_Count e Publisher non nulli
        {
            "$match": {
                "Critic_Count": {"$ne": None},
                "Publisher": {"$ne": None}
            }
        },
        # 2. Raggruppa per Publisher e somma Critic_Count
        {
            "$group": {
                "_id": "$Publisher",
                "total_Critic_Count": {"$sum": "$Critic_Count"}
            }
        },
        # 3. Ordina per somma decrescente
        {
            "$sort": {"total_Critic_Count": -1}
        },
        # 4. Proietta output pulito
        {
            "$project": {
                "_id": 0,
                "Publisher": "$_id",
                "total_Critic_Count": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline))


def get_all_developer2024(collection: Collection):
    """
    Retrieve the list of unique ratings from the MongoDB collection.
    :param collection: The MongoDB collection to query.
    :return: A sorted list of unique ratings.
    """
    return sorted(collection.distinct("developer"))


def get_all_developer2016(collection: Collection):
    """
    Retrieve the list of unique ratings from the MongoDB collection.
    :param collection: The MongoDB collection to query.
    :return: A sorted list of unique ratings.
    """
    return sorted(collection.distinct("Developer"))



def get_all_genres(collection: Collection):
    """
    Retrieve the list of unique genres from the MongoDB collection.
    :param collection: the MongoDB collection to query.
    :return: A sorted list of unique genres.
    """
    return sorted(collection.distinct("genre"))
