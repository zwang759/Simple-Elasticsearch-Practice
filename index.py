import json
import re
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Index, Document, Text, Keyword, Integer
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import tokenizer, analyzer
from elasticsearch_dsl.query import MultiMatch, Match


# Connect to local host server
connections.create_connection(hosts=['127.0.0.1'])

# Create elasticsearch object
es = Elasticsearch()
#
# Define analyzers appropriate for your data.
# You can create a custom analyzer by choosing among elasticsearch options
# or writing your own functions.
# Elasticsearch also has default analyzers that might be appropriate.
text_analyzer = analyzer('custom',
                       tokenizer='standard',
                       filter=['lowercase', 'stop', 'asciifolding', 'porter_stem'])
simple_analyzer = analyzer('custom', tokenizer='whitespace', filter=['lowercase'])
runtime_analyzer = analyzer('simple', tokenizer='whitespace', ignore_malformed=True)

# --- Add more analyzers here ---
# use stopwords... or not?
# use stemming... or not?

# Define document mapping (schema) by defining a class as a subclass of Document.
# This defines fields and their properties (type and analysis applied).
# You can use existing es analyzers or use ones you define yourself as above.
class Movie(Document):
    title = Text(analyzer=simple_analyzer)
    text = Text(analyzer=text_analyzer)
    starring = Text(analyzer=simple_analyzer)
    runtime = Integer(analyzer=runtime_analyzer)
    director = Text(analyzer=simple_analyzer)
    location = Keyword(analyzer=simple_analyzer)
    category = Keyword(analyzer=simple_analyzer)
    language = Keyword(analyzer=simple_analyzer)
    time = Keyword(analyzer=simple_analyzer)
    country = Keyword(analyzer=simple_analyzer)

    # --- Add more fields here ---
    # What data type for your field? List?
    # Which analyzer makes sense for each field?

    # override the Document save method to include subclass field definitions
    def save(self, *args, **kwargs):
        return super(Movie, self).save(*args, **kwargs)


# Populate the index
def buildIndex():
    """
    buildIndex creates a new film index, deleting any existing index of
    the same name.
    It loads a json file containing the movie corpus and does bulk loading
    using a generator function.
    """
    film_index = Index('sample_film_index')
    if film_index.exists():
        film_index.delete()  # Overwrite any previous version
    film_index.create()

    # Open the json film corpus
    with open('2018_movies.json', 'r', encoding='utf-8') as data_file:
        # load movies from json file into dictionary
        movies = json.load(data_file)
        size = len(movies)

    if es.indices.exists(index='sample_film_index'):
        es.indices.delete(index='sample_film_index') # Overwrite any previous version



    # Action series for bulk loading with helpers.bulk function.
    # Implemented as a generator, to return one movie with each call.
    # Note that we include the index name here.
    # The Document type is always 'doc'.
    # Every item to be indexed must have a unique key.
    def actions():
        # mid is movie id (used as key into movies dictionary)
        # There exist a issue in corpus that some empty values are "[]" instead of an empty list
        # The data type is inconsistent.
        # Need to clean the corpus
        for mid in range(1, size+1):
            if movies[str(mid)]['Starring'] == "[]":
                movies[str(mid)]['Starring'] = []
            if movies[str(mid)]['Country'] == "[]":
                movies[str(mid)]['Country'] = []
            if movies[str(mid)]['Language'] == "[]":
                movies[str(mid)]['Language'] = []
            if movies[str(mid)]['Director'] == "[]":
                movies[str(mid)]['Director'] = []
            if movies[str(mid)]['Running Time'] == "[]":
                movies[str(mid)]['Running Time'] = []
            elif movies[str(mid)]['Running Time'] == "TBA":
                movies[str(mid)]['Running Time'] = []
            elif movies[str(mid)]['Running Time'] == "? minutes":
                movies[str(mid)]['Running Time'] = []
            elif movies[str(mid)]['Running Time'] == "minutes":
                movies[str(mid)]['Running Time'] = []
            if len(movies[str(mid)]['Title']) <= 1:
                movies[str(mid)]['Title'] = "".join(movies[str(mid)]['Title'])
            else:
                movies[str(mid)]['Title'] = ", ".join(movies[str(mid)]['Title'])
            if len(movies[str(mid)]['Starring']) <= 1:
                movies[str(mid)]['Starring'] = "".join(movies[str(mid)]['Starring'])
            else:
                movies[str(mid)]['Starring'] = ", ".join(movies[str(mid)]['Starring'])
            if len(movies[str(mid)]['Director']) <= 1:
                movies[str(mid)]['Director'] = "".join(movies[str(mid)]['Director'])
            else:
                movies[str(mid)]['Director'] = ", ".join(movies[str(mid)]['Director'])
            if len(movies[str(mid)]['Time']) <= 1:
                movies[str(mid)]['Time'] = "".join(movies[str(mid)]['Time'])
            else:
                movies[str(mid)]['Time'] = ", ".join(movies[str(mid)]['Time'])
            if len(movies[str(mid)]['Location']) <= 1:
                movies[str(mid)]['Location'] = "".join(movies[str(mid)]['Location'])
            else:
                movies[str(mid)]['Location'] = ", ".join(movies[str(mid)]['Location'])
            if len(movies[str(mid)]['Language']) <= 1:
                movies[str(mid)]['Language'] = "".join(movies[str(mid)]['Language'])
            else:
                movies[str(mid)]['Language'] = ", ".join(movies[str(mid)]['Language'])
            if len(movies[str(mid)]['Country']) <= 1:
                movies[str(mid)]['Country'] = "".join(movies[str(mid)]['Country'])
            else:
                movies[str(mid)]['Country'] = ", ".join(movies[str(mid)]['Country'])
            if len(movies[str(mid)]['Categories']) <= 1:
                movies[str(mid)]['Categories'] = "".join(movies[str(mid)]['Categories'])
            else:
                movies[str(mid)]['Categories'] = ", ".join(movies[str(mid)]['Categories'])

            yield {
            "_index": "sample_film_index",
            "_type": 'doc',
            "_id": mid,
            "title":movies[str(mid)]['Title'],
            "starring":movies[str(mid)]['Starring'],
            "runtime":movies[str(mid)]['Running Time'],
            #movies[str(mid)]['runtime'] # You would like to convert runtime to integer (in minutes)
            # --- Add more fields here ---
            "director":movies[str(mid)]['Director'],
            "location":movies[str(mid)]['Location'],
            "time":movies[str(mid)]['Time'],
            "language":movies[str(mid)]['Language'],
            "categories":movies[str(mid)]['Categories'],
            "country":movies[str(mid)]['Country'],
            "text": movies[str(mid)]['Text'],
            }

    helpers.bulk(es, actions()) 


def test_analyzer(text, analyzer):
    """
    you might want to test your analyzer after you define it
    :param text: a string
    :param analyzer: the analyzer you defined
    :return: list of tokens processed by analyzer
    """
    output = analyzer.simulate(text)
    return [t.token for t in output.tokens]


# command line invocation builds index and prints the running time.
def main():
    start_time = time.time()
    buildIndex()
    print("=== Built index in %s seconds ===" % (time.time() - start_time))
        
if __name__ == '__main__':
    main()