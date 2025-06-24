from typing import Collection


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


def get_all_ratings(collection: Collection):
    """
    Retrieve the list of unique ratings from the MongoDB collection.
    :param collection: The MongoDB collection to query.
    :return: A sorted list of unique ratings.
    """
    return sorted(collection.distinct("Rating"))


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
