import getopt
import math
import os
import re
import sys
import time
import concurrent.futures

import pandas as pd

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
from dateutil import tz

ENV_DEFAULT_PATH = r'/media/miguel/Desarrollo/neoauto-project/.env'


def create_list_links(url):
    links_pageable = []
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    value = soup.find("div", class_="s-results__count")
    cant_results = int(re.findall(r'\d+', value.text)[0])

    cant_pages = math.ceil(cant_results / 20)
    for page in range(1, cant_pages + 1):
        links_pageable.append(url + f'?page={page}')
    return cant_results, links_pageable


def get_articles_from_list_link(url_base, url_list):
    autos = []
    for url in url_list:
        autos += get_articles_from_link(url_base, url)
    return autos


def get_articles_from_link(url_base, url):
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    articles = soup.find_all("article", class_="c-results-used")
    autos = []
    for article in articles:
        link = article.find('a', class_='c-results-use__link')['href']
        link = url_base + link
        print(f'Processing: {link}')
        soup_ = BeautifulSoup(requests.get(link).text, 'html.parser')
        date = datetime.now(tz=tz.gettz('America/Lima')).strftime("%Y-%m-%d %H:%M:%S")
        data_auto = dict()
        meta_content = soup_.find_all('div', class_='idSOrq')
        content = soup_.find_all('div', class_='htOtEa')
        meta_specs = soup_.find_all('div', class_="cLLifQ")
        specs = soup_.find_all('div', class_='jhOymW')

        data_auto['ID'] = link.split('-')[-1]
        data_auto['Fecha'] = date
        data_auto['Precio'] = soup_.find('p', class_='dYanzN').text
        for key, value in zip(meta_content, content):
            data_auto[key.text] = value.text
        for key, value in zip(meta_specs, specs):
            data_auto[key.text] = value.text
        data_auto['URL'] = link

        # Agregar clase Auto y libreria bunch, con modificacion
        # https://stackoverflow.com/questions/1305532/how-to-convert-a-nested-python-dict-to-object/31569634#31569634

        autos.append(data_auto)
    return autos


def to_save(data_csv, results, user, password, host, port, database, table):
    engine = create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))

    columns = ['ID', 'Fecha', 'Precio', 'Año Modelo', 'Kilometraje', 'Transmisión', 'Combustible',
               'Cilindrada', 'Categoría', 'Marca', 'Modelo', 'Año de fabricación',
               'Número de puertas', 'Tracción', 'Color', 'Número cilindros', 'Placa', 'URL'
               ]
    df = pd.DataFrame.from_dict(results)
    print(f'Exporting the data in a csv file in path: {data_csv}')
    df.to_csv(data_csv, index=False, header=True, columns=columns)
    print(f'Save the data in a sql table in host-database-table: {host},{database},{table}')
    df.to_sql(name=table, con=engine, if_exists='append', index=False, chunksize=1000)


def chunks(lst, n):
    # Separa la cantidad de enlaces en chunks de igual tamaño
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def prepare_list_process(url, search_csv):
    # Ejecucion en varios hilos
    list_links = []
    cant_results = 0
    df_search = pd.read_csv(search_csv)
    searchs = list(df_search.itertuples(index=False))
    print(f'Calculating quantity of articles to process')
    for value in searchs:
        marca = value[0].replace(' ', '-').lower()
        modelo = value[1].replace(' ', '-').lower()
        compound_url = f'{url}-{marca}-{modelo}'
        temp_cant_results, temp_list_links = create_list_links(compound_url)
        cant_results += temp_cant_results
        list_links += temp_list_links
    print(f'Number of articles to process: {cant_results}')
    return list_links


def get_env_parameters(argv: list):
    path = ENV_DEFAULT_PATH
    options, args = getopt.getopt(argv[1:], "e:", ["env ="])
    for name, value in options:
        if name in ['-e', '--env']:
            path = value
    return path


def main_multi(url_base, url, search_csv, data_csv, user, password, host, port, database, table, number_process=4):
    data_results: list[dict] = []
    list_links = prepare_list_process(url, search_csv)
    list_chunks = chunks(list_links, math.ceil(len(list_links) / number_process))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(lambda p: get_articles_from_list_link(*p), [url_base, chunk]) for chunk in
                   list_chunks]
        temp_results = [f.result() for f in futures]
        for result in temp_results:
            data_results += result

    to_save(data_csv, data_results, user, password, host, port, database, table)


def main_single(url_base, url, search_csv, data_csv, user, password, host, port, database, table):
    data_results: list[dict] = []
    # Ejecucion en un unico hilo
    list_links = prepare_list_process(url, search_csv)
    # print(list_links, sep='\n')
    for link in list_links:
        data_results += get_articles_from_link(url_base, link)

    to_save(data_csv, data_results, user, password, host, port, database, table)


if __name__ == '__main__':
    env_path = get_env_parameters(sys.argv)
    print("Importing environment values")
    if load_dotenv(env_path):
        URL_BASE = os.getenv('URL_BASE')
        URL = os.getenv('URL')
        SEARCH_CSV = os.getenv('SEARCH_CSV')
        DATA_CSV = os.getenv('DATA_CSV')
        NUMBER_PROCESS = int(os.getenv('NUMBER_PROCESS'))
        NUMBER_ARTICLES_PER_PAGE = int(os.getenv('NUMBER_ARTICLES_PER_PAGE'))
        USER_DATABASE = os.getenv('USER_DATABASE')
        PASSWORD_DATABASE = os.getenv('PASSWORD_DATABASE')
        HOST_DATABASE = os.getenv('HOST_DATABASE')
        PORT_DATABASE = os.getenv('PORT_DATABASE')
        NAME_DATABASE = os.getenv('NAME_DATABASE')
        NAME_TABLE = os.getenv('NAME_TABLE')
    else:
        print(f'ERROR. env file not found in the default path: {env_path}')
        exit()

    # start_time = time.time()
    # main_single(URL_BASE, URL, SEARCH_CSV, DATA_CSV,
    #            USER_DATABASE, PASSWORD_DATABASE, HOST_DATABASE, PORT_DATABASE, NAME_DATABASE)
    # print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    main_multi(URL_BASE, URL, SEARCH_CSV, DATA_CSV,
               USER_DATABASE, PASSWORD_DATABASE, HOST_DATABASE, PORT_DATABASE, NAME_DATABASE, NAME_TABLE,
               number_process=NUMBER_PROCESS)
    print("--- %s seconds ---" % (time.time() - start_time))
