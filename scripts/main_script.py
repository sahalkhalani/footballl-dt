import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
from sklearn import preprocessing
import requests
from bs4 import BeautifulSoup
import yaml
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, select, and_
from io import StringIO
import os
from datetime import datetime

host = "127.0.0.1"
user = "root"
password = "SahalAdmin#1234"
database = "football"

file_path = 'E1.csv'

with open("config.yaml", 'r') as configuration:
    config = yaml.safe_load(configuration)

def make_predictions():
    import_data()
    df = pd.read_csv(file_path)[['Div','Date','HomeTeam','AwayTeam','FTHG','FTAG','HTHG','HTAG','HS','AS','HST','AST','HC','AC','HY','AY','HR','AR', 'HTR','FTR']]
    # df = pd.read_csv(file_path)[['Div', 'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'HTHG', 'HTAG', 'HS', 'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A', 'IWH', 'IWD', 'IWA', 'PSH', 'PSD', 'PSA', 'WHH', 'WHD', 'WHA', 'VCH', 'VCD', 'VCA', 'MaxH', 'MaxD', 'MaxA', 'AvgH', 'AvgD', 'AvgA', 'B365>2.5', 'B365<2.5', 'P>2.5', 'P<2.5', 'Max>2.5', 'Max<2.5', 'Avg>2.5', 'Avg<2.5', 'AHh', 'B365AHH', 'B365AHA', 'PAHH', 'PAHA', 'MaxAHH', 'MaxAHA', 'AvgAHH', 'AvgAHA', 'B365CH', 'B365CD', 'B365CA', 'IWCH', 'IWCD', 'IWCA', 'PSCH', 'PSCD', 'PSCA', 'WHCH', 'WHCD', 'WHCA', 'VCCH', 'VCCD', 'VCCA', 'MaxCH', 'MaxCD', 'MaxCA', 'AvgCH', 'AvgCD', 'AvgCA', 'B365C>2.5', 'B365C<2.5', 'PC>2.5', 'PC<2.5', 'MaxC>2.5', 'MaxC<2.5', 'AvgC>2.5', 'AvgC<2.5', 'AHCh', 'B365CAHH', 'B365CAHA', 'PCAHH', 'PCAHA', 'MaxCAHH', 'MaxCAHA', 'AvgCAHH', 'AvgCAHA', 'HTR', 'FTR']]
    
    data = df[['Div', 'Date','HomeTeam','AwayTeam','FTR']]
    data['league'] = data['Div'].replace(['D1','E0', 'F1', 'SP1', 'I1'], ['Bundesliga', 'PremierLeague', 'Ligue1', 'LaLiga', 'SerieA'])
    df = df.drop(columns=['Div'])
    data = data.drop(columns=['Div'])
    data['Date'] = data['Date'].str.replace("/", "-")
    data = data.rename(columns = {'FTR': "result"})

    fixtures = fixtures_scrapping()
    
    fixtures = pd.concat([data, fixtures], ignore_index=True)

    X = df.iloc[:, :-1]
    y = df.iloc[:, -1]

    X = encode_non_numeric_values(X)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=52)

    clf = DecisionTreeClassifier()

    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)

    pred_vars_array =[]
    for index, row in fixtures.iterrows():
        pred_vars_array.append(get_prediction_values(row["HomeTeam"], row["AwayTeam"]))

    pred_vars = pd.concat(pred_vars_array, ignore_index=True)

    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy}")
    
    new_data = pd.DataFrame(pred_vars)
    prediction = clf.predict(new_data)
    fixtures["prediction"] = prediction

    ##### to store the results in CSV file
    # csv_file_path = ".//result1.csv"
    # fixtures.to_csv(csv_file_path, index=False)

    insert_data_in_DB(fixtures)

def get_prediction_values(homeTeam, awayTeam):
    df = pd.read_csv(file_path)
    filtered_df = df[(df["HomeTeam"] == homeTeam) | (df["AwayTeam"] == awayTeam)]

    selected_columns = ['Date','HomeTeam','AwayTeam','FTHG','FTAG','HTHG','HTAG','HS','AS','HST','AST','HC','AC','HY','AY','HR','AR', 'HTR']
    # selected_columns = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'HTHG', 'HTAG', 'HS', 'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A', 'IWH', 'IWD', 'IWA', 'PSH', 'PSD', 'PSA', 'WHH', 'WHD', 'WHA', 'VCH', 'VCD', 'VCA', 'MaxH', 'MaxD', 'MaxA', 'AvgH', 'AvgD', 'AvgA', 'B365>2.5', 'B365<2.5', 'P>2.5', 'P<2.5', 'Max>2.5', 'Max<2.5', 'Avg>2.5', 'Avg<2.5', 'AHh', 'B365AHH', 'B365AHA', 'PAHH', 'PAHA', 'MaxAHH', 'MaxAHA', 'AvgAHH', 'AvgAHA', 'B365CH', 'B365CD', 'B365CA', 'IWCH', 'IWCD', 'IWCA', 'PSCH', 'PSCD', 'PSCA', 'WHCH', 'WHCD', 'WHCA', 'VCCH', 'VCCD', 'VCCA', 'MaxCH', 'MaxCD', 'MaxCA', 'AvgCH', 'AvgCD', 'AvgCA', 'B365C>2.5', 'B365C<2.5', 'PC>2.5', 'PC<2.5', 'MaxC>2.5', 'MaxC<2.5', 'AvgC>2.5', 'AvgC<2.5', 'AHCh', 'B365CAHH', 'B365CAHA', 'PCAHH', 'PCAHA', 'MaxCAHH', 'MaxCAHA', 'AvgCAHH', 'AvgCAHA', 'HTR']
    result_df = filtered_df[selected_columns]

    result_df = encode_non_numeric_values(result_df)
    average_values = result_df.mean()

    predication_variables = pd.DataFrame(average_values).transpose()

    return predication_variables

def encode_non_numeric_values(value_to_be_encoded):
    le = preprocessing.LabelEncoder()
    encoded_result = value_to_be_encoded.apply(lambda col: le.fit_transform(col) if col.dtype == 'O' else col)
    return encoded_result

def fixtures_scrapping():
    try:
        leagues = ['Bundesliga', 'PremierLeague', 'Ligue1', 'LaLiga', 'SerieA']
        fixtures= pd.DataFrame()
        for league in leagues:
            page = requests.get(config['scraping'][league])
            soup = BeautifulSoup(page.content, 'html.parser')
            scraping_teams = soup.findAll("div", { "class" : "fixres__item"})
            number_of_matches = len(scraping_teams)

            dates_of_match = []
            scraping_home_teams = []
            scraping_away_teams = []
            for i in range(number_of_matches):
                dates_of_match.append(change_date_to_timestamp(scraping_teams[i].find_previous('h4', { "class" : "fixres__header2"}).text, scraping_teams[i].find_previous('h3', { "class" : "fixres__header1"}).text.split(" ")[1]))
                scraping_home_teams.append(scraping_teams[i].findAll("span", { "class" : "swap-text__target"})[0].text)
                scraping_away_teams.append(scraping_teams[i].findAll("span", { "class" : "swap-text__target"})[1].text)

            del scraping_teams
            scraping_matches = pd.DataFrame([dates_of_match, scraping_home_teams, scraping_away_teams]).T
            scraping_matches.columns = ['Date', 'HomeTeam','AwayTeam']
            scraping_matches['league'] = league
            fixtures = pd.concat([fixtures, scraping_matches], ignore_index=True)
        return fixtures
    except Exception as e:
        print(f"Exception Found: {e}")
        return None

def change_date_to_timestamp(date_string, year):
    components = date_string.split()
    day = components[1].rstrip('stndrdth') if len(components) > 1 else ''
    month = components[-1] if len(components) > 2 else ''

    if day and day.endswith(('st', 'nd', 'rd', 'th')):
        day = day.rstrip('stndrdth')

    formatted_date = f'{day} {month} {year}'
    parsed_date = datetime.strptime(formatted_date, '%d %B %Y')

    sql_timestamp = parsed_date.strftime('%d-%m-%Y')
    return sql_timestamp

def insert_data_in_DB(prediction_result):
    try:
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
        table_name = config['result_table_name']
        metadata = MetaData()
        all_league_names = list(prediction_result['league'].unique())

        prediction_result['Date'] = pd.to_datetime(prediction_result['Date'], format='%d-%m-%Y')
        table = Table(table_name, metadata, autoload_with=engine)

        with engine.connect() as connection:
            future_date_condition = table.c.Date >= datetime.now().date()
            condition = and_(table.c.league.in_(all_league_names))
            connection.execute(table.delete())
            connection.commit()

        prediction_result.to_sql(table_name, con=engine, if_exists='append', index=False)

    except mysql.connector.Error as err:
        print(f"Error while connecting DB: {err}")

def import_data():
    leagues = list(config['csv_name'])
    if os.path.exists(file_path):
        os.remove(file_path)
    for league in leagues:
        url = 'https://www.football-data.co.uk/mmz4281/' + config['season'] + config['csv_name'][league]
        req = requests.get(url)

        new_data = pd.read_csv(StringIO(req.text))
        columns = ['Div', 'Date', 'Time', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'HTHG', 'HTAG', 'HTR', 'HS', 'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC', 'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A', 'IWH', 'IWD', 'IWA', 'PSH', 'PSD', 'PSA', 'WHH', 'WHD', 'WHA', 'VCH', 'VCD', 'VCA', 'MaxH', 'MaxD', 'MaxA', 'AvgH', 'AvgD', 'AvgA', 'B365>2.5', 'B365<2.5', 'P>2.5', 'P<2.5', 'Max>2.5', 'Max<2.5', 'Avg>2.5', 'Avg<2.5', 'AHh', 'B365AHH', 'B365AHA', 'PAHH', 'PAHA', 'MaxAHH', 'MaxAHA', 'AvgAHH', 'AvgAHA', 'B365CH', 'B365CD', 'B365CA', 'IWCH', 'IWCD', 'IWCA', 'PSCH', 'PSCD', 'PSCA', 'WHCH', 'WHCD', 'WHCA', 'VCCH', 'VCCD', 'VCCA', 'MaxCH', 'MaxCD', 'MaxCA', 'AvgCH', 'AvgCD', 'AvgCA', 'B365C>2.5', 'B365C<2.5', 'PC>2.5', 'PC<2.5', 'MaxC>2.5', 'MaxC<2.5', 'AvgC>2.5', 'AvgC<2.5', 'AHCh', 'B365CAHH', 'B365CAHA', 'PCAHH', 'PCAHA', 'MaxCAHH', 'MaxCAHA', 'AvgCAHH', 'AvgCAHA', 'FTR']

        try:
            existing_data = pd.read_csv(file_path)
        except FileNotFoundError:
            existing_data = pd.DataFrame(columns=columns)

        new_data_filtered = new_data[columns]
        combined_data = pd.concat([existing_data, new_data_filtered], ignore_index=True)
        combined_data.to_csv(file_path, index=False)
    
    replace_values_in_csv(file_path)

def replace_values_in_csv(csv_path):
    replacement_values = {
        'Man United': 'Manchester United',
        'Man City': 'Manchester City',
        'Tottenham':'Tottenham Hotspur',
        'Wolves': 'Wolverhampton Wanderers',
        'Luton':'Luton Town',
        'West Ham': 'West Ham United',
        'Brighton': 'Brighton and Hove Albion',
        'Paris SG': 'Paris Saint-Germain'
    }

    df = pd.read_csv(csv_path)

    df.replace(replacement_values, inplace=True)

    df.to_csv(csv_path, index=False)

make_predictions()

# fixtures_scrapping("PremierLeague")
# import_data()

# def compare_csv_files(file1, file2):
    # leagues = list(config['csv_name'])
    # print(f"LEAGUES >> {leagues}")

    # df1 = pd.read_csv(file1)
    # df2 = pd.read_csv(file2)

    # merged = pd.merge(df1, df2, how='outer', indicator=True)
    # differences = merged[merged['_merge'] != 'both']

    # if differences.empty:
    #     print("The CSV files are identical.")
    # else:
    #     print("The CSV files are different. Differences:")
    #     print(differences)

