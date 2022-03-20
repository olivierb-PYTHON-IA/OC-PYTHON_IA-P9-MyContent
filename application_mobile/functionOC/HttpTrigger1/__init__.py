import logging
import azure.functions as func
import json
import pandas as pd
from io import BytesIO


def get_most_popular_category_for_user(user_id, df_data):
    most_popular_category_for_user = df_data[df_data['user_id']==user_id].groupby(['category_id']).size().sort_values(ascending=False).index[0]
    return most_popular_category_for_user


def get_most_popular_articles_for_category(category_id, df_data):
    most_popular_articles_for_category = df_data[df_data['category_id']==category_id].groupby(['article_id']).size().sort_values(ascending=False).index.to_list()
    return most_popular_articles_for_category


def get_articles_already_read_for_user(user_id, df_data):
    articles_already_read = df_data[df_data['user_id']==user_id]['article_id'].unique().tolist()
    return articles_already_read


def get_recommendations_articles_category(user_id, top_n, df_data):
    most_popular_category_for_user = get_most_popular_category_for_user(user_id, df_data)
    most_popular_articles_for_category = get_most_popular_articles_for_category(most_popular_category_for_user, df_data)
    articles_already_read_for_user = get_articles_already_read_for_user(user_id, df_data)    
    most_popular_articles_for_category_not_already_read = [i for i in most_popular_articles_for_category if i not in set(articles_already_read_for_user)]
    top_n_most_popular_articles_for_category_not_already_read = most_popular_articles_for_category_not_already_read[:top_n]
    return top_n_most_popular_articles_for_category_not_already_read


def main(req: func.HttpRequest, data: func.InputStream) -> func.HttpResponse:
    req_body = req.get_json()
    id_bookshelf = int(req_body.get('userId'))

    #There are 10 000 users in the Bookshelf application
    if id_bookshelf < 10000:
        df_data = pd.read_csv(BytesIO(data.read()))
        #As there are 10 000 users in the Bookshelf application, we keep the first 10 000 users in our dataset 
        user_id = sorted(df_data['user_id'].unique().tolist())[:10000][id_bookshelf]
        nb_recommandations = 5
        recommandations = get_recommendations_articles_category(user_id, nb_recommandations, df_data)
        return func.HttpResponse(json.dumps(recommandations), mimetype="application/json", status_code=200)
    else:
        return func.HttpResponse("User Id is not correct. Please try with another User ID", status_code=400)
