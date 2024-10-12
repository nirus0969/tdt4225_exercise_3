import os
from pprint import pprint
from DbConnector import DbConnector
from datetime import datetime
from itertools import islice
from haversine import haversine


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        # Comment out the two lines below the comment after running the insertion function: 
        # insert_activities(), insert_users(), and insert_trackpoints()
        self.users_with_labels: list[str] = self.initialize_users_with_labels()
        self.valid_files: dict[str, bool] = self.initialize_valid_files()

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
                                        "activity_id": activity_id,
                                        "user_id": user
                                    }
                                    trackpoints_to_insert.append(
                                        trackpoint_document)
        batch_size = 10000
        count = 0
        if trackpoints_to_insert:
            for i in range(0, len(trackpoints_to_insert), batch_size):
                count += batch_size
                print(str(count) + "/" + str(len(trackpoints_to_insert)))
                batch = trackpoints_to_insert[i:i + batch_size]
                self.db.TrackPoint.insert_many(batch)
            print("Inserted trackpoints into MongoDB.")

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find().limit(10)
        for doc in documents:
            pprint(doc)
            print()

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['local_db'].list_collection_names()
        print(collections)

    def task_1(self, collection_names=["User", "Activity", "TrackPoint"]):
        for collection in collection_names:
            count = self.db[collection].count_documents({})
            print(f'{collection.capitalize()} Count: {count}')

    def task_2(self):
        pipeline = [
            {
                '$project': {
                    'numActivities': {'$size': '$activities'}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'averageActivities': {'$avg': '$numActivities'}
                }
            }
        ]
        result = list(self.db.User.aggregate(pipeline))
        print(f'Average number of activities per user: {result[0]['averageActivities']}')

    def task_3(self):
        pipeline = [
            {
                '$project': {
                    '_id': 1,
                    'numActivities': {'$size': '$activities'}
                }
            },
            {
                '$sort': {
                    'numActivities': -1
                }
            },
            {
                '$limit': 20
            }
        ]

        top_users = list(self.db.User.aggregate(pipeline))

        for user in top_users:
            pprint(user)

    def task_4(self):
        distinct_users = self.db.Activity.distinct(
            'user_id', {'transportation_mode': 'taxi'})
        for user in distinct_users:
            pprint(user)

    def task_5(self):
        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': None}
                }
            },
            {
                '$group': {
                    '_id': '$transportation_mode',
                    'count': {'$sum': 1}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'mode_of_transport': '$_id',
                    'total_activities': '$count'
                }
            }
        ]

        transport_modes = list(self.db.Activity.aggregate(pipeline))

        for mode in transport_modes:
            pprint(mode)

    def task_6a(self):
        pipeline = [
            {
                "$project": {
                    "start_year": {"$year": "$start_date_time"},
                    "end_year": {"$year": "$end_date_time"}
                }
            },
            {
                "$project": {
                    "years": {
                        "$cond": {
                            "if": {"$eq": ["$start_year", "$end_year"]},
                            "then": ["$start_year"],
                            "else": ["$start_year", "$end_year"]
                        }
                    }
                }
            },
            {
                "$unwind": "$years"
            },
            {
                "$group": {
                    "_id": "$years",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]

        result = list(self.db.Activity.aggregate(pipeline))

        for doc in result:
            pprint(doc)

    def task_6b(self):
        total_hours = {}
        rows = list(self.db.Activity.find(
            {}, {'_id': 1, 'start_date_time': 1, 'end_date_time': 1}))

        for row in rows:
            id = row['_id']
            start_year = row['start_date_time'].year
            end_year = row['end_date_time'].year

            if start_year == end_year:
                hours = (row['end_date_time'] -
                         row['start_date_time']).total_seconds() / 3600
                total_hours[start_year] = total_hours.get(
                    start_year, 0) + hours
            else:
                end_of_start_year = datetime(start_year, 12, 31, 23, 59, 59)
                hours_start_year = (
                    end_of_start_year - row['start_date_time']).total_seconds() / 3600
                total_hours[start_year] = total_hours.get(
                    start_year, 0) + hours_start_year

                start_of_end_year = datetime(end_year, 1, 1, 0, 0, 0)
                hours_end_year = (row['end_date_time'] -
                                  start_of_end_year).total_seconds() / 3600
                total_hours[end_year] = total_hours.get(
                    end_year, 0) + hours_end_year
        sorted_total_hours = sorted(
            total_hours.items(), key=lambda x: x[1], reverse=True)
        pprint(sorted_total_hours)

    def task_7(self):

        activity_ids = list(self.db.Activity.find(
            {'user_id': '112', 'transportation_mode': 'walk'},
            {'_id': 1}
        ))

        activity_id_list = [activity['_id'] for activity in activity_ids]

        result = self.db.TrackPoint.aggregate([
            {
                '$match': {
                    '$expr': {
                        '$eq': [{'$year': '$date_time'}, 2008]
                    },
                    'activity_id': {
                        '$in': activity_id_list
                    }
                }
            },
            {
                '$lookup': {
                    'from': 'Activity',
                    'localField': 'activity_id',
                    'foreignField': '_id',
                    'as': 'activity'
                }
            },
            {
                '$unwind': '$activity'
            },
            {
                '$project': {
                    'coordinates': 1,
                    'user_id': '$activity.user_id',
                    'date_time': 1,
                    'transportation_mode': '$activity.transportation_mode',
                    'activity_id': '$activity._id'
                }
            },
            {
                '$sort': {
                    'activity_id': -1,
                    'date_time': 1
                }
            }
        ])

        distances = {}
        last_activity_id = None
        last_coordinates = None

        for row in result:
            lat, lon = row['coordinates']
            activity_id = row['activity_id']
            coordinates = (lat, lon)

            if activity_id not in distances:
                distances[activity_id] = 0.0
                last_coordinates = None

            if last_coordinates is not None and last_activity_id == activity_id:
                distance = haversine(last_coordinates, coordinates)
                distances[activity_id] += distance

            last_coordinates = coordinates
            last_activity_id = activity_id

        print(f"Total distance walked in 2008 by user 112: {sum(distances.values())} km")

    def task_8(self):
        result = self.db.TrackPoint.aggregate([
            {
                '$match': {
                    'altitude': {'$ne': -777}
                }
            },
            {
                '$project': {
                    'altitude': 1,
                    'user_id': 1,
                    'date_time': 1,
                    'activity_id': 1
                }
            },
            {
                '$sort': {
                    'user_id': 1,
                    'activity_id': 1,
                    'date_time': 1
                }
            }
        ])
        altitude_gain = {}
        last_user = None
        last_altitude = None
        last_activity = None
        for row in result:
            user = row['user_id']
            activity = row['activity_id']
            altitude = row['altitude']

            if user not in altitude_gain:
                altitude_gain[user] = 0.0
                last_altitude = None
                last_activity = None

            if last_user == user and last_activity != activity:
                last_altitude = None

            if last_user == user and last_activity == activity:
                if last_altitude is not None and last_altitude < altitude:
                    altitude_gain[user] += altitude - last_altitude

            last_altitude = altitude
            last_user = user
            last_activity = activity

        sorted_altitude_gain = sorted(
            altitude_gain.items(), key=lambda x: x[1], reverse=True)
        pprint(sorted_altitude_gain[:20])

    def task_9(self):
        result = self.db.TrackPoint.aggregate([
            {
                '$project': {
                    'altitude': 1,
                    'user_id': 1,
                    'date_time': 1,
                    'activity_id': 1
                }
            },
            {
                '$sort': {
                    'user_id': 1,
                    'activity_id': 1,
                    'date_time': 1
                }
            }
        ])
        invalid_activity_count = {}

        prev_user_id = None
        prev_activity_id = None
        prev_date_time = None
        invalid_activity_flag = False

        for trackpoint in result:
            user_id = trackpoint['user_id']
            activity_id = trackpoint['activity_id']
            date_time = trackpoint['date_time']

            if user_id not in invalid_activity_count:
                invalid_activity_count[user_id] = 0

            if prev_user_id == user_id and prev_activity_id == activity_id:
                time_diff = (date_time - prev_date_time).total_seconds() / 60

                if time_diff >= 5:
                    invalid_activity_flag = True

            else:

                if invalid_activity_flag:
                    invalid_activity_count[prev_user_id] += 1

                invalid_activity_flag = False

            prev_user_id = user_id
            prev_activity_id = activity_id
            prev_date_time = date_time

        if invalid_activity_flag:
            invalid_activity_count[prev_user_id] += 1

        pprint(invalid_activity_count)

    def task_10(self):
        pipeline = [
            {
                '$match': {
                    'coordinates.0': {
                        '$gte': 39.916,
                        '$lt': 39.917
                    },
                    'coordinates.1': {
                        '$gte': 116.397,
                        '$lt': 116.398
                    }
                }
            },
            {
                '$lookup': {
                    'from': 'Activity',
                    'localField': 'activity_id',
                    'foreignField': '_id',
                    'as': 'activity'
                }
            },
            {
                "$group": {
                    "_id": "$activity.user_id",
                }
            },
        ]
        rows = self.db.TrackPoint.aggregate(pipeline)
        for row in rows:
            pprint(row)

    def task_11(self):
        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': None}
                }
            },
            {
                '$group': {
                    '_id': {
                        'user_id': '$user_id',
                        'transportation_mode': '$transportation_mode'
                    },
                    'count': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.user_id': 1,
                    'count': -1
                }
            },
            {
                '$group': {
                    '_id': '$_id.user_id',
                    'most_used_transportation_mode': {'$first': '$_id.transportation_mode'},
                    'count': {'$first': '$count'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'user_id': '$_id',
                    'most_used_transportation_mode': 1,
                }
            },
            {
                '$sort': {
                    'user_id': 1,
                }
            },
        ]
        rows = self.db.Activity.aggregate(pipeline)
        for row in rows:
            pprint(row)


def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_coll()
        program.insert_activities()
        program.insert_users()
        program.insert_trackpoints()
        program.task_1() # Change this to whatever task you want displayed

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
