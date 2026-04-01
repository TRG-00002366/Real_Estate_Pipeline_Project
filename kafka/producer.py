from faker import Faker
import random
import numpy as np
from datetime import timedelta, datetime
from kafka import KafkaProducer
import json
import time
import argparse

class RentalListingGen:
    def __init__(self):
        self.fake = Faker()

        #List of cities
        self.cities = [
            # Alabama
            "Birmingham",
            "Montgomery",
            # Alaska
            "Anchorage",
            "Fairbanks",
            # Arizona
            "Phoenix",
            "Tucson",
            # Arkansas
            "Little Rock",
            "Fayetteville",
            # California
            "Los Angeles",
            "San Francisco",
            # Colorado
            "Denver",
            "Colorado Springs",
            # Connecticut
            "Bridgeport",
            "New Haven",
            # Delaware
            "Wilmington",
            "Dover",
            # Florida
            "Miami",
            "Orlando",
            # Georgia
            "Atlanta",
            "Savannah",
            # Hawaii
            "Honolulu",
            "Hilo",
            # Idaho
            "Boise",
            "Idaho Falls",
            # Illinois
            "Chicago",
            "Springfield",
            # Indiana
            "Indianapolis",
            "Fort Wayne",
            # Iowa
            "Des Moines",
            "Cedar Rapids",
            # Kansas
            "Wichita",
            "Overland Park",
            # Kentucky
            "Louisville",
            "Lexington",
            # Louisiana
            "New Orleans",
            "Baton Rouge",
            # Maine
            "Portland",
            "Bangor",
            # Maryland
            "Baltimore",
            "Annapolis",
            # Massachusetts
            "Boston",
            "Worcester",
            # Michigan
            "Detroit",
            "Grand Rapids",
            # Minnesota
            "Minneapolis",
            "St. Paul",
            # Mississippi
            "Jackson",
            "Gulfport",
            # Missouri
            "Kansas City",
            "St. Louis",
            # Montana
            "Billings",
            "Missoula",
            # Nebraska
            "Omaha",
            "Lincoln",
            # Nevada
            "Las Vegas",
            "Reno",
            # New Hampshire
            "Manchester",
            "Nashua",
            # New Jersey
            "Newark",
            "Jersey City",
            # New Mexico
            "Albuquerque",
            "Santa Fe",
            # New York
            "New York",
            "Buffalo",
            # North Carolina
            "Charlotte",
            "Raleigh",
            # North Dakota
            "Fargo",
            "Bismarck",
            # Ohio
            "Columbus",
            "Cleveland",
            # Oklahoma
            "Oklahoma City",
            "Tulsa",
            # Oregon
            "Portland",
            "Eugene",
            # Pennsylvania
            "Philadelphia",
            "Pittsburgh",
            # Rhode Island
            "Providence",
            "Warwick",
            # South Carolina
            "Charleston",
            "Columbia",
            # South Dakota
            "Sioux Falls",
            "Rapid City",
            # Tennessee
            "Nashville",
            "Memphis",
            # Texas
            "Houston",
            "Dallas",
            # Utah
            "Salt Lake City",
            "Provo",
            # Vermont
            "Burlington",
            "Montpelier",
            # Virginia
            "Virginia Beach",
            "Richmond",
            # Washington
            "Seattle",
            "Spokane",
            # West Virginia
            "Charleston",
            "Morgantown",
            # Wisconsin
            "Milwaukee",
            "Madison",
            # Wyoming
            "Cheyenne",
            "Casper",
        ]
        self.city_weights = [self.fake.random_int(min=1, max=30) for i in range(100)]
        self.status_types = ("open", "rented")
        self.building_types = ["Apartment", "Single Family", "Other"]


        self.properties = []

        #Multiplier for size, based on bedrooms
        self.sizemulti = {
            1:0.33,
            2:0.5,
            3:0.75,
            4:1.0,
            5:1.3
        }
        for _ in range(2500):
            property_data = {
                "property_id": self.fake.unique.random_int(min=10000000, max=99999999),

                "building_type": random.choices(
                    ["Apartment", "Single Family", "Other"],
                    weights=[10, 6, 2],
                    k=1
                )[0],

                "year_built": self.fake.random_int(min=1950, max=2024),

                "bedrooms": int(min(np.random.geometric(p=0.35), 5)),

                "size": int(np.clip(np.random.normal(loc=2000, scale=300), 500, 5000)),

                "city": random.choices(self.cities, self.city_weights, k=1)[0],
            }
            property_data['size'] = int(property_data['size']*self.sizemulti[property_data["bedrooms"]])
            self.properties.append(property_data)


    def generate_listing(self):

        rental_status = random.choices(self.status_types, weights=[4, 10], k=1)[0]

        posted_on = self.fake.date_time_between(start_date="-10y", end_date="now")

        property_data = random.choice(self.properties)

        rented_on = None
        if rental_status == "rented":
            rented_on = posted_on + timedelta(days=self.fake.random_int(min=1, max=60))

        rent = int(np.clip(1000 + property_data['bedrooms'] * 300 + property_data['size'] * 0.8 + np.random.normal(0, 300),900,10000))
        rent = rent *(1.002**((posted_on.year-2000)*12+posted_on.month))
        listing = {
            "property_id": property_data["property_id"],
            "building_type": property_data["building_type"],
            "year_built": property_data["year_built"],
            "bedrooms": property_data["bedrooms"],
            "size": property_data["size"],
            "city": property_data["city"],
            "customer_id": self.fake.unique.random_int(min=10000000, max=99999999),
            "posted_on": posted_on.isoformat(),
            "rented_on": rented_on.isoformat() if rented_on else None,
            "rent" : rent,
            "duration": self.fake.random_element([6, 12, 18, 24]),
            "rental_status": rental_status,
        }
        return listing

    def post_listing(self):
        
        posted_on = datetime.now()
        property_data = random.choice(self.properties)

        listing = {
            "property_id": property_data["property_id"],
            "building_type": property_data["building_type"],
            "year_built": property_data["year_built"],
            "bedrooms": property_data["bedrooms"],
            "size": property_data["size"],
            "city": property_data["city"],
            "customer_id": self.fake.unique.random_int(min=10000000, max=99999999),
            "posted_on": posted_on.isoformat(),
            "rented_on": None,
            "rent" : int(np.clip(1000 + property_data['bedrooms'] * 300 + property_data['size'] * 0.8 + np.random.normal(0, 300),900,10000)),
            "duration": self.fake.random_element([6, 12, 18, 24]),
            "rental_status": "open",
        }
        return listing




def create_producer(bootstrap_servers: str = 'kafka:9092'):
    producer = KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        acks='all',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def main():
    producer = create_producer()
    listing_gen = RentalListingGen()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events",type=int,default=10)
    args = parser.parse_args()
    num_events = args.num_events

    
    for i in range(int(num_events)):
        producer.send('listing-events',listing_gen.generate_listing())
    producer.close()

if __name__ == "__main__":
    main()


