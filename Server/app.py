#!/ivo/bin/env python`
from flask import Flask, request
import json
import logging

from werkzeug.exceptions import BadRequestKeyError

SERVICE_DEBUG = True
app = Flask(__name__)

users = []
id_counter = 0


@app.route('/')
def hello_world():
    return 'Djordje Andjelkovic 1/18RI'


@app.route("/users/<id_user>", methods=['GET'])
def user_by_id(id_user):
    for user in users:
        if user['id'] == int(id_user):
            return user, 200
    return {}, 404


@app.route('/users', methods=['GET', 'POST'])
def users_route():
    if request.method == "GET":
        return {"users": users}, 200
    elif request.method == "POST":
        try:
            global id_counter
            for p in request.json.get("predmeti"):
                if "ime" not in p.keys() or "espb" not in p.keys():
                    raise BadRequestKeyError
            user = {
                "ime": request.json["ime"],
                "prezime": request.json["prezime"],
                "username": request.json.get("username"),
                "smer": request.json.get("smer"),
                "predmeti": [p for p in request.json.get("predmeti")],
                "id": id_counter
            }
            id_counter += 1
            logging.info("New user: " + json.dumps(user))
            users.append(user)
            return user, 201
        except (BadRequestKeyError, Exception, TypeError) as e:
            logging.error(e)
            return {"error": str(e)}, 400


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8081, debug=False)
