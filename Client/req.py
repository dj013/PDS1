import requests
import json

url = 'http://localhost:8081/users'
myobj = {
        "ime": "Djordje",
        "prezime": "Andjelkovic",
        "username": "dandjelkovic",
        "smer": "RI",
        "predmeti": [
                        {"ime": "PDS" , "espb": 8},
                        {"ime": "ORS" , "espb": 6},
                        {"ime": "OS" , "espb": 6},
                        {"ime": "nesto" , "espb": 6},]
}



x = requests.post(url,json=myobj)




