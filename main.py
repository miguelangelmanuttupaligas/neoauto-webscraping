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
    return links_pageable


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
        date = datetime.now(tz=tz.gettz('America/Lima')).strftime("%m-%d-%Y %H:%M:%S")
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

        autos.append(data_auto)
    return autos


def to_save(data_csv, results, user, password, host, port, database):
    engine = create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format('user', 'password', 'host', 'port', 'database'))

    columns = ['ID', 'Fecha', 'Precio', 'Año Modelo', 'Kilometraje', 'Transmisión', 'Combustible',
               'Cilindrada', 'Categoría', 'Marca', 'Modelo', 'Año de fabricación',
               'Número de puertas', 'Tracción', 'Color', 'Número cilindros', 'Placa', 'URL'
               ]
    df = pd.DataFrame.from_dict(results)
    df.to_csv(data_csv, index=False, header=True, columns=columns)
    # df.to_sql(name='nombre_tabla', con=engine, chunksize=1000, )


def chunks(lst, n):
    # Separa la fila de enlaces (20 articulos como maximo por pagina) en paquetes iguales
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main_multi(url_base, url, search_csv, data_csv, user, password, host, port, database, number_process=4):
    # Ejecucion en varios hilos
    list_links = []
    data_results: list[dict] = []
    df_search = pd.read_csv(search_csv)
    searchs = list(df_search.itertuples(index=False))
    for value in searchs:
        # temp_results: list[dict] = []
        marca = value[0].replace(' ', '-').lower()
        modelo = value[1].replace(' ', '-').lower()
        compound_url = f'{url}-{marca}-{modelo}'
        list_links += create_list_links(compound_url)
        # for link in list_links:
        #    temp_results += get_articles_from_link(link)
    print(len(list_links))
    print(list_links, sep='\n')
    list_chunks = chunks(list_links, math.ceil(len(list_links) / number_process))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # futures = [executor.submit(get_articles_from_list_link, chunk) for chunk in list_chunks]
        futures = [executor.submit(lambda p: get_articles_from_list_link(*p), [url_base, chunk]) for chunk in list_chunks]
        temp_results = [f.result() for f in futures]
        for result in temp_results:
            data_results += result

    to_save(data_csv, data_results, user, password, host, port, database)


def main_single(url_base, url, search_csv, data_csv, user, password, host, port, database):
    # Ejecucion en un unico hilo
    list_links = []
    data_results: list[dict] = []
    df_search = pd.read_csv(search_csv)
    searchs = list(df_search.itertuples(index=False))
    for value in searchs:
        # temp_results: list[dict] = []
        marca = value[0].replace(' ', '-').lower()
        modelo = value[1].replace(' ', '-').lower()
        compound_url = f'{url}-{marca}-{modelo}'
        list_links += create_list_links(compound_url)
        # for link in list_links:
        #    temp_results += get_articles_from_link(link)

    print(len(list_links))
    print(list_links, sep='\n')
    for link in list_links:
        data_results += get_articles_from_link(url_base, link)

    to_save(data_csv, data_results, user, password, host, port, database)


def get_env_parameters(argv: list):
    path = ENV_DEFAULT_PATH
    options, args = getopt.getopt(argv[1:], "e:", ["env ="])
    for name, value in options:
        if name in ['-e', '--env']:
            path = value
    return path


if __name__ == '__main__':
    URL_BASE = ''
    URL = ''
    SEARCH_CSV = ''
    DATA_CSV = ''
    NUMBER_PROCESS = 0
    NUMBER_ARTICLES_PER_PAGE = 0
    USER_DATABASE = ''
    PASSWORD_DATABASE = ''
    HOST = ''
    PORT = ''
    DATABASE = ''

    env_path = get_env_parameters(sys.argv)
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

    else:
        print(f'ERROR. env file not found in the default path: {env_path}')
        exit()

    print(URL_BASE, URL, SEARCH_CSV, DATA_CSV, NUMBER_PROCESS, NUMBER_ARTICLES_PER_PAGE)

    start_time = time.time()
    main_single(URL_BASE, URL, SEARCH_CSV, DATA_CSV,
                USER_DATABASE, PASSWORD_DATABASE, HOST_DATABASE, PORT_DATABASE, NAME_DATABASE)
    print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    main_multi(URL_BASE, URL, SEARCH_CSV, DATA_CSV,
               USER_DATABASE, PASSWORD_DATABASE, HOST_DATABASE, PORT_DATABASE, NAME_DATABASE, number_process=4)
    print("--- %s seconds ---" % (time.time() - start_time))
