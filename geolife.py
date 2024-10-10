import os
from pprint import pprint
from DbConnector import DbConnector
from datetime import datetime
from itertools import islice


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        #self.users_with_labels: list[str] = self.initialize_users_with_labels()
        #self.valid_files: dict[str, bool] = self.initialize_valid_files()

    def initialize_valid_files(self) -> dict[str, bool]:
        valid_files = {}
        for dirpath, dirnames, filenames in os.walk("dataset/dataset/Data"):
            for filename in filenames:
                if filename.endswith('.plt'):
                    full_path = os.path.join(dirpath, filename)
                    line_count = 0
                    with open(full_path, 'r') as file:
                        for line in file:
                            line_count += 1
                            if line_count > 2506:
                                valid_files[full_path] = False
                                break
                    if line_count <= 2506:
                        if line_count <= 6:
                            valid_files[full_path] = False
                        else:
                            valid_files[full_path] = True
        return valid_files

    def valid_file(self, filename: str) -> bool:
        return self.valid_files[filename]

    def initialize_users_with_labels(self) -> list[str]:
        users_with_labels = []
        with open("dataset/dataset/labeled_ids.txt", "r") as file:
            for line in file:
                users_with_labels.append(line.strip())
        return users_with_labels

    def user_has_labels(self, user_id: str) -> bool:
        return user_id in self.users_with_labels
    
    def find_matching_label(self, user: str, start_end_datetime: tuple[datetime, datetime]) -> str | None:
        filename = "dataset/dataset/Data/" + user + "/labels.txt"

        start_time, end_time = start_end_datetime

        with open(filename, 'r') as file:
            lines = file.readlines()

        for line in lines[1:]:
            column = line.strip().split('\t')
            label_start_time = datetime.strptime(
                column[0], '%Y/%m/%d %H:%M:%S')
            label_end_time = datetime.strptime(column[1], '%Y/%m/%d %H:%M:%S')
            transportation_mode = column[2]

            if label_start_time == start_time and label_end_time == end_time:
                return transportation_mode

        return None

    def get_first_last_datetime(self, filename: str) -> tuple[datetime, datetime]:
        first_datetime = None
        last_datetime = None

        with open(filename, 'r') as file:
            for line_number, line in enumerate(file):
                if line_number < 6:
                    continue
                line = line.strip()
                fields = line.split(",")
                if len(fields) < 7:
                    continue
                current_datetime = datetime.strptime(
                    f"{fields[-2]} {fields[-1]}", '%Y-%m-%d %H:%M:%S'
                )
                if first_datetime is None:
                    first_datetime = current_datetime
                last_datetime = current_datetime

        return first_datetime, last_datetime

    def create_coll(self):
        collection_names = ['User', 'Activity', 'TrackPoint']
        for name in collection_names:
            collection = self.db.create_collection(name)
            print('Created collection: ', collection)





    def insert_activities(self):
        activities_to_insert = []
        count = 0
        for dirpath, _, filenames in os.walk("dataset/dataset/Data"):
            user = dirpath[-14:-11]
            for filename in filenames:
                if filename.endswith('.plt'):
                    full_path = os.path.join(dirpath, filename)
                    if self.valid_file(filename=full_path):
                        count += 1
                        print(str(count) + "/16048")
                        start_end_datetime = self.get_first_last_datetime(
                            filename=full_path)
                        label = None
                        if self.user_has_labels(user_id=user):
                            label = self.find_matching_label(
                                user=user, start_end_datetime=start_end_datetime)
                        activity_document = {
                            "user_id": user,
                            "transportation_mode": label,
                            "start_date_time": start_end_datetime[0],
                            "end_date_time": start_end_datetime[1]
                        }
                        activities_to_insert.append(activity_document)
                                             
        batch_size = 1000
        count = 0
        if activities_to_insert:
            for i in range(0, len(activities_to_insert), batch_size):
                count += batch_size
                print(f"{count}/{len(activities_to_insert)}")
                batch = activities_to_insert[i:i + batch_size]
                self.db.Activity.insert_many(batch)
            print("Inserted activities into MongoDB.")



    def insert_users(self):
        count = 0
        directory = "dataset/dataset/Data"
        for entry in os.listdir(directory):
            full_path = os.path.join(directory, entry)
            if os.path.isdir(full_path):
                count += 1
                print(str(count) + "/182")
                activities_to_insert = []
                documents = self.db.Activity.find({"user_id": entry})
                for doc in documents:
                    activities_to_insert.append(doc['_id'])  
                has_label = self.user_has_labels(user_id=entry)
                activity_document = {
                        "_id": entry,
                        "has_labels": has_label,
                        "activities": activities_to_insert,
                    }
                self.db.User.insert_one(activity_document)



    def insert_trackpoints(self):
        trackpoints_to_insert = []
        count = 0
        for dirpath, _, filenames in os.walk("dataset/dataset/Data"):
            user = dirpath[-14:-11]
            for filename in filenames:
                if filename.endswith('.plt'):
                    full_path = os.path.join(dirpath, filename)
                    if self.valid_file(filename=full_path):
                        count += 1
                        print(str(count) + "/16048")
                        start_end_datetime = self.get_first_last_datetime(
                            filename=full_path)
                        query = {
                            "user_id": user,
                            "start_date_time": start_end_datetime[0],
                            "end_date_time": start_end_datetime[1]
                        }                       
                        activity = self.db.Activity.find_one(query, {"_id": 1})
                        if activity:
                            activity_id = activity['_id']
                            with open(full_path, 'r') as file:
                                for line in islice(file, 6, None):  # Skip first 6 lines
                                    line = line.strip()
                                    fields = line.split(",")
                                    if len(fields) < 7:
                                        continue
                                    datetime_obj = datetime.strptime(
                                        f"{fields[5]} {fields[6]}", '%Y-%m-%d %H:%M:%S')
                                    trackpoint_document = {
                                        "coordinates": [float(fields[0]), float(fields[1])],
                                        "altitude": float(fields[3]),
                                        "date_time": datetime_obj,
                                        "activity_id": activity_id
                                    }        
                                    trackpoints_to_insert.append(trackpoint_document)                              
        batch_size = 10000
        count = 0
        if trackpoints_to_insert:
            for i in range(0, len(trackpoints_to_insert), batch_size):
                count += batch_size
                print(str(count) + "/" + str(len(trackpoints_to_insert)))
                batch = trackpoints_to_insert[i:i + batch_size]
                self.db.TrackPoint.insert_many(batch)
            print("Inserted trackpoints into MongoDB.")
                

    def count(self, collection_names):  
        for collection in collection_names:
            count = self.db[collection].count_documents({})
            print(f'{collection.capitalize()} Count: {count}')  

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find().limit(10)
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['local_db'].list_collection_names()
        print(collections)


def main():
    program = None
    try:
        program = ExampleProgram()
        program.count(collection_names=["User", "Activity", "TrackPoint"])
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
