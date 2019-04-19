"""
This module implements a (partial, sample) query interface for elasticsearch movie search. 
You will need to rewrite and expand sections to support the types of queries over the fields in your UI.

Documentation for elasticsearch query DSL:
https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html

For python version of DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/

Search DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
"""

import re
from flask import *
from index import Movie
from pprint import pprint
from elasticsearch_dsl.utils import AttrList
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

app = Flask(__name__)

# Initialize global variables for rendering page
tmp_text = ""
tmp_title = ""
tmp_star = ""
tmp_min = ""
tmp_max = ""
gresults = {}

# display query page
@app.route("/")
def search():
    return render_template('page_query.html')

# display results page for first set of results and "next" sets.
@app.route("/results", defaults={'page': 1}, methods=['GET', 'POST'])
@app.route("/results/<page>", methods=['GET', 'POST'])
def results(page):
    global tmp_text
    global tmp_title
    global tmp_star
    global tmp_director
    global tmp_language
    global tmp_location
    global tmp_time
    global tmp_categories
    global tmp_country
    global tmp_min
    global tmp_max
    global gresults

    # convert the <page> parameter in url to integer.
    if type(page) is not int:
        page = int(page.encode('utf-8'))    
    # if the method of request is post (for initial query), store query in local global variables
    # if the method of request is get (for "next" results), extract query contents from client's global variables  
    if request.method == 'POST':
        text_query = request.form['query']
        star_query = request.form['starring']
        director_query = request.form['director']
        language_query = request.form['language']
        location_query = request.form['location']
        time_query = request.form['time']
        categories_query = request.form['categories']
        country_query = request.form['country']
        mintime_query = request.form['mintime']

        if len(mintime_query) is 0:
            mintime = 0
        else:
            if mintime_query.replace('.', '', 1).isdigit():
                mintime = float(mintime_query)
            else:
                return render_template('error_page.html')

        maxtime_query = request.form['maxtime']
        if len(maxtime_query) is 0:
            maxtime = 99999
        else:
            if maxtime_query.replace('.', '', 1).isdigit():
                maxtime = float(maxtime_query)
            else:
                return render_template('error_page.html')

        # update global variable template data
        tmp_text = text_query
        tmp_star = star_query
        tmp_director = director_query
        tmp_language = language_query
        tmp_location = location_query
        tmp_time = time_query
        tmp_categories = categories_query
        tmp_country = country_query
        tmp_min = mintime
        tmp_max = maxtime
    else:
        # use the current values stored in global variables.
        text_query = tmp_text
        star_query = tmp_star
        director_query = tmp_director
        language_query = tmp_language
        location_query = tmp_location
        time_query = tmp_time
        categories_query = tmp_categories
        country_query = tmp_country
        mintime = tmp_min

        if tmp_min > 0:
            mintime_query = tmp_min
        else:
            mintime_query = ""
        maxtime = tmp_max
        if tmp_max < 99999:
            maxtime_query = tmp_max
        else:
            maxtime_query = ""
    
    # store query values to display in search boxes in UI
    shows = {}
    shows['text'] = text_query
    shows['starring'] = star_query
    shows['director'] = director_query
    shows['language'] = language_query
    shows['location'] = location_query
    shows['time'] = time_query
    shows['categories'] = categories_query
    shows['maxtime'] = maxtime_query
    shows['mintime'] = mintime_query
       
    # Create a search object to query our index 
    search = Search(index='sample_film_index')

    # Build up your elasticsearch query in piecemeal fashion based on the user's parameters passed in.
    # The search API is "chainable".
    # Each call to search.query method adds criteria to our growing elasticsearch query.
    # You will change this section based on how you want to process the query data input into your interface.
        
    # search for runtime using a range query
    s = search.query('range', runtime={'gte': mintime, 'lte': maxtime})
    
    # Conjunctive search over multiple fields (title and text) using the text_query passed in
    if len(text_query) > 0:
        s = s.query('multi_match', query=text_query, type='cross_fields', fields=['title', 'text'], operator='and')
        response = s.execute()
        if len(response) == 0:
            s = search.query('range', runtime={'gte': mintime, 'lte': maxtime})
            s = s.query('multi_match', query=text_query, type='cross_fields', fields=['title^4', 'text'],
                        operator='or')
        phrase = re.findall(r'"(.*?)"', text_query)
        if len(phrase) != 0:
            s = s.query(Q('match_phrase', text=phrase[0]))

    # search for matching stars
    # You should support multiple values (list)
    if len(star_query) > 0:
        s = s.query('match', starring=star_query)
    if len(director_query) > 0:
        s = s.query('match', director=director_query)
    if len(language_query) > 0:
        s = s.query('match', language=language_query)
    if len(location_query) > 0:
        s = s.query('match', location=location_query)
    if len(time_query) > 0:
        s = s.query('match', time=time_query)
    if len(categories_query) > 0:
        s = s.query('match', categories=categories_query)
    if len(country_query) > 0:
        s = s.query('match', categories=country_query)
    
    # highlight
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')
    # s = s.highlight('text', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('title', fragment_size=999999999, number_of_fragments=1)
    for key in shows:
        s = s.highlight(key, fragment_size=999999999, number_of_fragments=1)

    # determine the subset of results to display (based on current <page> value)
    start = 0 + (page-1)*10
    end = 10 + (page-1)*10
    
    # execute search and return results in specified range.
    response = s[start:end].execute()

    # insert data into response
    resultList = {}
    for hit in response.hits:
        result={}
        result['score'] = hit.meta.score
        
        if 'highlight' in hit.meta:
            if 'title' in hit.meta.highlight:
                result['title'] = hit.meta.highlight.title[0]
            else: 
                result['title'] = hit.title

            if 'starring' in hit.meta.highlight:
                result['starring'] = hit.meta.highlight.starring[0]
            else:
                result['starring'] = hit.starring

            if 'runtime' in hit.meta.highlight:
                result['runtime'] = hit.meta.highlight.runtime[0]
            else:
                result['runtime'] = hit.runtime

            if 'director' in hit.meta.highlight:
                result['director'] = hit.meta.highlight.director[0]
            else:
                result['director'] = hit.director

            if 'location' in hit.meta.highlight:
                result['location'] = hit.meta.highlight.location[0]
            else:
                result['location'] = hit.location

            if 'time' in hit.meta.highlight:
                result['time'] = hit.meta.highlight.time[0]
            else:
                result['time'] = hit.time

            if 'language' in hit.meta.highlight:
                result['language'] = hit.meta.highlight.language[0]
            else:
                result['language'] = hit.language

            if 'categories' in hit.meta.highlight:
                result['categories'] = hit.meta.highlight.categories[0]
            else:
                result['categories'] = hit.categories

            if 'country' in hit.meta.highlight:
                result['country'] = hit.meta.highlight.country[0]
            else:
                result['country'] = hit.country
            if 'text' in hit.meta.highlight:
                result['text'] = hit.meta.highlight.text[0]
            else:
                result['text'] = hit.text
                
        else:
            result['title'] = hit.title
            result['starring'] = hit.starring
            result['runtime'] = hit.runtime
            result['director'] = hit.director
            result['location'] = hit.location
            result['time'] = hit.time
            result['language'] = hit.language
            result['categories'] = hit.categories
            result['country'] = hit.country
            result['text'] = hit.text

        resultList[hit.meta.id] = result

    # make the result list available globally
    gresults = resultList
    
    # get the total number of matching results
    result_num = response.hits.total
      
    # if we find the results, extract title and text information from doc_data, else do nothing
    if result_num > 0:
        return render_template('page_SERP.html', results=resultList, res_num=result_num, page_num=page, queries=shows)
    else:
        message = []
        if len(text_query) > 0:
            message.append('Unknown search term: ' + text_query)
        if len(star_query) > 0:
            message.append('Cannot find star: ' + star_query)
        if len(time_query) > 0:
            message.append('Cannot find time: ' + time_query)
        if len(director_query) > 0:
            message.append('Cannot find director: ' + director_query)
        if len(location_query) > 0:
            message.append('Cannot find location: ' + location_query)
        if len(language_query) > 0:
            message.append('Cannot find language: ' + language_query)
        if len(categories_query) > 0:
            message.append('Cannot find categories: ' + categories_query)
        if len(country_query) > 0:
            message.append('Cannot find country: ' + country_query)

        if len(mintime_query) > 0 and len(maxtime_query) > 0:
            message.append('Cannot find running time between {} mins and {} mins'.format(mintime_query, maxtime_query))
        elif len(mintime_query) > 0:
            message.append('Cannot find running time greater than {} mins'.format(mintime_query))
        else:
            message.append('Cannot find running time less than {} mins'.format(maxtime_query))
        
        return render_template('page_SERP.html', results=message, res_num=result_num, page_num=page, queries=shows)

# display a particular document given a result number
@app.route("/documents/<res>", methods=['GET'])
def documents(res):
    global gresults
    film = gresults[res]
    filmtitle = film['title']
    for term in film:
        if type(film[term]) is AttrList:
            s = "\n"
            for item in film[term]:
                s += item + ",\n "
            film[term] = s
    # fetch the movie from the elasticsearch index using its id
    movie = Movie.get(id=res, index='sample_film_index')
    filmdic = movie.to_dict()
    film['runtime'] = str(filmdic['runtime']) + " min"
    return render_template('page_targetArticle.html', film=film, title=filmtitle)

if __name__ == "__main__":
    # app.run()
    app.run(debug=True, use_reloader=False)