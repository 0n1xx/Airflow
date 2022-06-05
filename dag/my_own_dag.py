from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'v-saharov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 5),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -429299660
BOT_TOKEN = Variable.get('telegram_secret')

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f"Well Done! Dag {dag_id} completed on {date}. It's fine."
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args, catchup=False)
def lesson3_v_saharov_20():
    
    @task(retries=3)
    def get_data():
        year = 1994 + hash(f'{"v-saharov-20"}') % 23
        games = pd.read_csv("vgsales.csv").query("Year == @year")
        return games
    
    #Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=4)
    def top_game(games):
        top_game_df = games.groupby("Name",as_index=False).agg({"Global_Sales":"sum"})\
    .sort_values("Global_Sales",ascending=False).head(1).Name
        return top_game_df
    
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=4)
    def top_genres(games):
        top_genres_df = games.groupby("Genre",as_index=False).agg({"EU_Sales":"sum"})\
    .sort_values("EU_Sales",ascending=False).head(1).Genre
        return top_genres_df
    
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task(retries=4)
    def top_platform_na(games):
        top_platform_na_df = games.query("NA_Sales > 1").groupby("Platform",as_index = False)\
            .agg({"Name":pd.Series.nunique})\
            .sort_values("Name",ascending=False).head(1).Platform
        return top_platform_na_df
    
    #У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task(retries=4)
    def top_publisher_japan(games):
        top_publisher_japan_df = games.groupby("Publisher",as_index = False).agg({"JP_Sales":"mean"})\
            .sort_values("JP_Sales",ascending=False).head(1).Publisher
        return top_publisher_japan_df
    
    #Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=4)
    def europe_bigger_japan(games):
        europe_bigger_japan_df = games.groupby("Name",as_index = False)\
            .agg({"EU_Sales":"sum","JP_Sales":"sum"})\
            .query("EU_Sales > JP_Sales")\
            .shape[0]
        return europe_bigger_japan_df
    
    @task(on_success_callback=send_message)
    def print_data(top_game_df, top_genres_df, top_platform_na_df, top_publisher_japan_df, europe_bigger_japan_df):
        context = get_current_context()
        date = context['ds']
        year = 1994 + hash('v-saharov-20') % 23

        print(f"The most popular game in {year} is {top_game_df}")
        print(f"The most popular genres in Europe in {year} is {top_genres_df}")
        print(f"The most popular platform with the biggest amount of games which have been sold more than 1 millions copies in {year} in NA is {top_platform_na_df}")
        print(f"The best publisher of mean sales in Japan in {year} is {top_publisher_japan_df}")
        print(f"{europe_bigger_japan_df} were sold more in Europe than in Japan in {year}")
        

    data = get_data()
    world_top_game = top_game(data)
    top_genres_eu = top_genres(data)
    top_platform_na_1m = top_platform_na(data)
    top_publisher_japan_sales = top_publisher_japan(data)
    number_europe_bigger_japan = europe_bigger_japan(data)
    print_data(world_top_game, top_genres_eu, top_platform_na_1m, top_publisher_japan_sales, number_europe_bigger_japan)
    
lesson3_v_saharov_20 = lesson3_v_saharov_20()

