import os
from flask import Flask, jsonify, request, Response
from prometheus_flask_exporter import PrometheusMetrics
from base.tmdbclient import TmdbClient
from base.mongoclient import MongoClient
from base.rabbitmq_client import RabbitMqClient
from recommendations import Recommendations
from watchlist import Watchlist

app = Flask(__name__)
metrics = PrometheusMetrics(app)

# custom metric to be applied to multiple endpoints
common_counter = metrics.counter(
    'by_endpoint_counter', 'Request count by endpoints',
    labels={'endpoint': lambda: request.endpoint, 'status': lambda resp: resp.status_code}
)


# we define the route /
@app.route('/')
def welcome():
    # return a json
    return jsonify({'status': 'api is working'})


# we define the route /
@app.route('/mongo_ping')
async def mongo_test():
    ping_response, ping_status = await MongoClient().ping()
    # return a json
    return Response(ping_response, status=ping_status)


# we define the route /
@app.route('/tmdb_ping')
async def tmdb_test():
    ping_response, ping_status = await TmdbClient().ping()
    # return a json
    return Response(ping_response, status=ping_status)


# we define the route /
@app.route('/rmq_ping')
async def rmq_test():
    ping_response, ping_status = await RabbitMqClient().ping()
    # return a json
    return Response(ping_response, status=ping_status)


# we define the route /
@app.route('/get_reccomendations', methods=['GET', 'POST'])
async def get_reccs():
    print("Request received to get recommendations...")
    user_id = request.json.get('user_id')
    if user_id:
        result, error = await Recommendations().calculate_reccs(user_id=user_id)
        # return a json
        if error:
            return {'status': str(error)}
        return {'result': result}

    return jsonify({'status': False})


@app.route('/get_watchlist', methods=['GET', 'POST'])
async def get_watchlist():
    print("Request received to get watchlist...")
    print(request.json)
    user_id = request.json.get('user_id')
    if user_id:
        print(f"Request received to get watchlist for user {user_id}...")
        movie_list = request.json.get('movie_list')
        result, error = await Watchlist().process_watchlist(movie_list=movie_list)
        # return a json
        if error:
            return {'status': str(error)}
        return {'result': result}

    return jsonify({'status': False})


if __name__ == '__main__':
    # define the localhost ip andd the cport that is going to be used
    # in some future article, we are going to use an env variable instead a hardcoded port
    app.run(host='0.0.0.0', port=os.getenv('PORT'), debug=False)
