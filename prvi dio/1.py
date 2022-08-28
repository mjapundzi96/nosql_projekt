import time
from typing import Dict, List
from pymongo import ASCENDING, DESCENDING, MongoClient
import csv
import pandas as pd
from bson.objectid import ObjectId


class Projekt:
    def __init__(self) -> None:
        self.client = MongoClient('mongodb://127.0.0.1:27017/?readPreference=primary&appname=MongoDB+Compass&directConnection=true&ssl=false')
        self.database = self.client['baza1']
        self.collection = self.database['csm']
        self.categorical_var = ["Year","Genre","Sequel"]
        self.continuous_var = ["Ratings","Gross","Budget","Screens","Sentiment","Views","Likes","Aggregate Followers","Dislikes","Comments"]
        self.database["frekvencija_csm"].drop()
        self.database["statistika_csm"].drop()
        self.database["statistika_1_csm"].drop()
        self.database["statistika_2_csm"].drop()
        self.database["emb_csm"].drop()
        self.database["emb2_csm"].drop()

    def zadatak1(self):
        for column in self.collection.find_one():
            if column in self.categorical_var:
                query = {column:""}
                new_values = { "$set": { column: "empty" } }
                result = self.collection.update_one(query, new_values)
            elif column in self.continuous_var:
                query = {column:float('NaN')}
                new_values = { "$set": { column: -1 } }
                result = self.collection.update_one(query, new_values)

    def zadatak2(self):
        statistika_csm = self.database["statistika_csm"]
        for column in self.continuous_var:
            result = list(self.collection.aggregate([
                    {
                        '$project': {
                            column: 1
                        }
                    }, {
                        '$group': {
                            '_id': column, 
                            f"avg_{column}": {
                                '$avg':f"${column}" 
                            },
                            f"std_dev_{column}": {
                                '$stdDevPop':f"${column}"
                            }
                            
                        }
                    }
                ]))[0]
            statistika_csm.insert_one(
                {
                    "Varijabla":column,
                 "Srednja vrijednost":result[f"avg_{column}"],
                 "Standardna devijacija":result[f"std_dev_{column}"],
             
                    }
                )
            
    def zadatak3(self):
        frekvencija_csm = self.database["frekvencija_csm"]
        for column in self.categorical_var:
            frekvencija_csm.insert_one({"Varijabla":column})
            distinct_values = self.collection.distinct(column)
            for value in distinct_values:
                frequency = list(self.collection.aggregate([{"$match": {column: value}}, {"$count": column} ]))[0][column]
                frekvencija_csm.update_one({"Varijabla":column},{"$inc":{str(value):frequency}})
                
    def zadatak4(self):
        statistika1_csm = self.database["statistika_1_csm"]
        statistika2_csm = self.database["statistika_2_csm"]

        for row in self.database["statistika_csm"].find():
            avg = row['Srednja vrijednost']
            for row2 in self.collection.find():
                self.database[
                    "statistika_1_csm"
                         if row2[row["Varijabla"]] <= avg
                            else "statistika_2_csm"].insert_one(
                        {
                            "Varijabla":row["Varijabla"],
                            "Vrijednost":row2[row["Varijabla"]],
                            "Srednja vrijednost":avg
                        }
                    )
                
    def zadatak5(self):
        collection = list(self.collection.find())
        for row in self.database["frekvencija_csm"].find():
            for row2 in collection:
                row2[row["Varijabla"]] = {"Vrijednost":row2[row["Varijabla"]],"Frekvencije":row}

        self.database['emb_csm'].insert_many(collection)

    def zadatak6(self):
        collection = list(self.collection.find())
        for row in self.database["statistika_csm"].find():
            for row2 in collection:
                row2[row["Varijabla"]] = {"Vrijednost":row2[row["Varijabla"]],"Statistika":row}

        self.database['emb2_csm'].insert_many(collection)

    def zadatak7(self):
        for column in self.continuous_var:
           for row in self.database['emb2_csm'].find():
               if "Statistika" in row[column]:
                   avg = row[column]["Statistika"]["Srednja vrijednost"]
                   st_dev = row[column]["Statistika"]["Standardna devijacija"]
                   if st_dev > avg + avg * 0.1:
                        query = {'_id':ObjectId(row['_id'])}
                        new_values = { "$set": { column: "" } }
                        result = self.database['emb2_csm'].update_one(query, new_values)
                   
               
    def zadatak8(self):
        self.collection.create_index([("Gross",ASCENDING),("Likes",DESCENDING)])
        new_collection = self.collection.find().sort([("Gross",ASCENDING),("Likes",DESCENDING)])
        self.database['slozeni_index'].insert_many(new_collection)

if __name__ == "__main__":

    projekt = Projekt()
    
    projekt.zadatak1()

    projekt.zadatak2()

    projekt.zadatak3()

    projekt.zadatak4()

    projekt.zadatak5()

    projekt.zadatak6()

    projekt.zadatak7()

    projekt.zadatak8()

















