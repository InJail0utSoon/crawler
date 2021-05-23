import sys
import requests
import logging
import json
import pymongo
from kafka import KafkaConsumer, KafkaProducer
from url_normalize import url_normalize
import urllib
from bs4 import BeautifulSoup
from bs4.element import Comment
from datetime import datetime
from traceback import print_exc
from elasticsearch import Elasticsearch

from middlewares import MongoMiddleware
from validators import (ESConfigValidator, KafkaConfigValidator, MongoDBConfigValidator)
from exceptions import (InvalidAppConfigException, InvalidValidatorException, AppConfigBuildingException)

logger = logging.getLogger('spider')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

DEFAULT_CONFIG_LOCATION = "./config.json"
VALIDATOR_KEY = 'validators'

request_headers = {
    "Accept" : "*/*",
    "Accept-Encoding" : "gzip, deflate, br",	
    "Accept-Language" : "en-US,en;q=0.5",
    "Connection" : "keep-alive",
    "Referer" : "https://www.google.com",
    "User-Agent" : "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"	
        }

class KafkaConnection:
    def __init__(self, consumer, producer):
        self.consumer = consumer
        self.producer = producer


def connect_to_mongo(app_config):
    db_server_address = app_config['mongo']['db_server_address']
    db_port = app_config['mongo']['db_port']
    db_name = app_config['mongo']['db_name']
    client = pymongo.MongoClient(db_server_address, db_port)
    db = client[db_name]
    return db


def connect_to_es(app_config):
    db_server_address = app_config['es']['db_server_address']
    db_port = app_config['es']['db_port']
    verify_certs = app_config['es']['verify_certs']
    return Elasticsearch([f'{db_server_address}:{db_port}'], verify_certs=verify_certs)


def connect_to_kafka(app_config):
    crawler_topic = app_config['kafka']['concerned_topic']
    kafka_broker_url = app_config['kafka']['kafka_broker_url']
    consumer = KafkaConsumer(crawler_topic, bootstrap_servers=kafka_broker_url, value_deserializer=json.loads)
    producer = KafkaProducer(bootstrap_servers=kafka_broker_url)
    return KafkaConnection(consumer, producer)


class Webpage:
    
    def __init__(self, url, content, timestamp):
        self.url = url
        self.content = content
        self.timestamp = timestamp
    
    def persist(self, es_connection, mongo_connection):
        logger.info("persisting url "+self.url)
        res = mongo_connection['web_data'].insert_one({"_id":self.url, "content":self.content, "timestamp":datetime.now()})
        logger.info("mongo persist res -> "+str(res))
        res = es_connection.index(index='web_data', id=self.url, body={'url':self.url, 'content':self.content, 'timestamp':self.timestamp}) 
        logger.info("es persist res -> "+res['result'])


class Task:
    
    def __init__(self, url):
        self.raw_url = url
        self.url = url_normalize(url)
        self.domain = urllib.parse.urlparse(url).netloc


class Spider:
    
    def __init__(self, mongo_client, es_client, kafka_connection, request_headers, url_checker_middlewares):
        self.es_client = es_client
        self.mongo_client = mongo_client
        self.kafka_consumer = kafka_connection.consumer
        self.kafka_producer = kafka_connection.producer
        self.request_headers = request_headers
        self.url_checker_middlewares = url_checker_middlewares


    def check_connections(self):
        return True

    def push_new_tasks(self, urls):
        for url in urls:
            logger.info("pushing new task -> "+url)
            data = json.dumps({"url": url})
            self.kafka_producer.send("web-crawler-queue", bytes(data, encoding='utf-8')).get(timeout=10)

    def check_need_to_crawl_url(self, url):
        for layer in self.url_checker_middlewares:
            if layer.check(url):
                return False
        return True
    
    def tag_visible(self, elem):
        if elem.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(elem, Comment):
            return False
        return True
    

    def is_valid(self, url):
        """
        Checks whether `url` is a valid URL.
        """
        parsed = urllib.parse.urlparse(url)
        valid_schemes = ['http', 'https']
        return bool(parsed.netloc) and bool(parsed.scheme) and parsed.scheme in valid_schemes


    def crawl(self, task):
        head_res = requests.head(task.url, headers=request_headers)
        
        if head_res.status_code == 200:
            
            res = requests.get(task.url, headers=request_headers)
            html_content = res.content
            soup = BeautifulSoup(html_content, 'html.parser')
            texts = soup.findAll(text=True)
            visible_texts = filter(self.tag_visible, texts)
            content = u" ".join(text.strip() for text in visible_texts)
            webpage = Webpage(task.url, content, datetime.now())
            webpage.persist(self.es_client, self.mongo_client)
            urls = set()

            for a_tag in soup.findAll("a"):
                href = a_tag.attrs.get("href")
                if href == "" or href is None:
                    continue
                href = urllib.parse.urljoin(task.url, href)
                parsed_href = urllib.parse.urlparse(href)
                href = parsed_href.scheme + "://" +parsed_href.netloc + parsed_href.path
                if self.is_valid(href):
                    urls.add(href)
            
            if task.url in urls: 
                urls.remove(task.url)

            filtered_urls = filter(self.check_need_to_crawl_url, urls)
            self.push_new_tasks(filtered_urls)

        else :
            logger.error(f'{task.url} responded with {head_res.status_code}')
        

    def run(self):
        logger.info('running the spider')
        for record in self.kafka_consumer:
            try:
                task = Task(record.value['url'])
                logger.info("new task ==> "+task.url)
                self.check_connections()
                self.crawl(task)

            except Exception as ex:
                logger.error(ex)
                print_exc()


def __main__(app_config):
    try:
        mongo_db_connection = connect_to_mongo(app_config)
        es_connection = connect_to_es(app_config)
        kafka_connection = connect_to_kafka(app_config)
        url_checking_middlewares = [MongoMiddleware(mongo_db_connection)]

        spider = Spider(mongo_db_connection, es_connection, kafka_connection, request_headers, url_checking_middlewares)
        spider.run()

    except Exception as ex:
        print('spider faced some execption')
        print_exc()


def validate_config(app_config, validators):
    for validator in validators:
        logger.info(str(validator) + " -> " + str(validator.is_valid(app_config)))
        if not validator.is_valid(app_config):
            raise InvalidAppConfigException


def recognize_validators(app_config):

    validator_recognizer = {
            'mongo': MongoDBConfigValidator(),
            'es' : ESConfigValidator(),
            'kafka' : KafkaConfigValidator()
            }

    validators = []
    if VALIDATOR_KEY in app_config.keys():
        for validator_name in app_config[VALIDATOR_KEY]:
            if validator_name in validator_recognizer.keys():
                validators.append(validator_recognizer[validator_name])
            else :
                raise InvalidValidatorException
    return validators


def build_config(argv):
    try:
        config_file_path = DEFAULT_CONFIG_LOCATION
        if len(argv) > 1:
            config_file_path = argv[1]
        config_file = open(config_file_path)
        config_str = ""
        for line in config_file:
            config_str += line
        app_config = json.loads(config_str)

    except Exception as ex:
       print_exc()
       raise AppConfigBuildingException
    validators = recognize_validators(app_config)
    validate_config(app_config, validators)
    return app_config
    

if __name__ == '__main__':
    __main__(build_config(sys.argv))
