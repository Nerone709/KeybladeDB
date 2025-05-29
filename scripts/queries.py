from pandas import DataFrame

def get_videogames_by_rating(rating, collection):
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

def get_sales_by_developer(collection_2024, developers):
    """"
    Retrieve total global sales for a specific developer.
    :param collection_2024: The MongoDB collection for the year 2024.
    :param developers: List of developers to filter by (e.g. 'Nintendo').
    :return: A list of dictionaries containing the name, developer, platform, and global sales of the video games.
    """
    pipeline = [
        {"$match": {"developer": {"$in": developers}}},
        {"$project": {
            "_id": 0,
            "title": 1,
            "developer": 1,
            "console": 1,
            "total_sales": 1
        }},
        {"$sort": {"total_sales": -1}}, # Sort by Global_Sales in descending order
    ]

    result = list(collection_2024.aggregate(pipeline))
    return result

def get_sales_by_genre(collection, genre):
    """"
    Retrieve total global sales for a specific genre.
    :param collection: The MongoDB collection for the year 2024.
    :param genre: The genre to filter by (e.g. 'Action', 'Adventure').
    :return: A list of dictionaries containing the name, genre, platform, and global sales of the video games.
    """
    pipeline = [
        {"$match": {"Genre": genre}},
        {"$project": {
            "_id": 0,
            "Name": 1,
            "Platform": 1,
            "Year_of_Release": 1,
            "Genre": 1,
            "Publisher": 1,
            "Global_Sales": 1
        }},
        {"$sort": {"Global_Sales": -1}} # Sort by Global_Sales in descending order
    ]

    result = list(collection.aggregate(pipeline))
    return result

def get_num_videogames_by_publisher(collection, publisher, year):
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
                "$gte" : year
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