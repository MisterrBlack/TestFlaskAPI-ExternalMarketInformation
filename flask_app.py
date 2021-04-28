# -*- coding: utf-8 -*-
"""
Python Flask Application with entry points:
- /v1/quotes POST : call stock api and get all information of clients stock prices 
- /v1/quotes GET: get all stock prices from clients
- /v1/news POST : call news api and get all information regarding the keywords and customers. 
- /v1/news GET: get all existing news 
Develop to build from source directly (i.e., source to image builder with dependencies, etc.)
Developed as Python package, including package and dependencies with requirements.txt
"""

# Import packages
from flask import Flask, request, jsonify, abort, make_response
import get_articles_eventRegistry
import get_stock_prices

# initialize Flask-app
app = Flask(__name__)
# app.config["DEBUG"] = True  # for Debugging and Development


@app.route('/', methods=['GET'])
def home():
    return "API to get external market information."

@app.route('/api/v1/quotes', methods=['GET'])
def get_quotes():
    # get all stock prices from clients
    results = get_stock_prices.get_stock_prices_from_file()
    if results.empty:
        abort(404)
    # Python dataframe to the JSON format.
    return make_response(results.to_json(orient="records"), 200)

@app.route('/api/v1/quotes', methods=['POST'])
def post_quotes():
    # call stock api and get all information of clients stock prices 
    results = get_stock_prices.get_new_stock_prices_from_api()
    if results.empty:
        abort(404)
    # Python dataframe to the JSON format.
    return make_response(results.to_json(orient="records"), 201)

@app.route('/api/v1/news', methods=['GET'])
def get_news():
    # get all existing news
    results = get_articles_eventRegistry.get_articles_from_file()
    if results.empty:
        abort(404)
    # Python dataframe to the JSON format.
    return make_response(results.to_json(orient="records"), 200)

@app.route('/api/v1/news', methods=['POST'])
def post_news():
    # call news api and get all information regarding the keywords and customers. 
    results = get_articles_eventRegistry.get_new_articles_from_eventRegistry()
    if results.empty:
        abort(404)
    # Python dataframe to the JSON format.
    return make_response(results.to_json(orient="records"), 201)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == "__main__":
    app.run()

