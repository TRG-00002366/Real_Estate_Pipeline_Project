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

    def generate_listing(self):

        rental_status = random.choices(self.status_types, weights=[4, 10], k=1)[0]

        building_type = random.choices(self.building_types, weights=[10, 6, 2], k=1)[0]

        posted_on = self.fake.date_time_between(start_date="-2y", end_date="now")

        rented_on = None
        if rental_status == "rented":
            rented_on = posted_on + timedelta(days=self.fake.random_int(min=1, max=60))

        listing = {
            "property_id": self.fake.unique.random_int(min=10000000, max=99999999),
            "customer_id": self.fake.unique.random_int(min=10000000, max=99999999),
            "building_type": building_type,
            "year_built": self.fake.random_int(min=1950, max=2024),
            "posted_on": posted_on.isoformat(),
            "rented_on": rented_on.isoformat() if rented_on else None,
            "bedrooms": min(np.random.geometric(p=0.35), 5),
            "rent": int(np.clip(np.random.normal(loc=2200, scale=500), 900, 10000)),
            "size": int(np.clip(np.random.normal(loc=2000, scale=300), 900, 5000)),
            "duration": self.fake.random_element([6, 12, 18, 24]),
            "city": random.choices(self.cities, self.city_weights, k=1)[0],
            "rental_status": rental_status,
        }
        return listing

    def post_listing(self):

        rental_status = "open"

        posted_on = datetime.now()
        rented_on = None
        listing = {
            "property_id": self.fake.unique.random_int(min=10000000, max=99999999),
            "customer_id": self.fake.unique.random_int(min=10000000, max=99999999),
            "year_built": self.fake.random_int(min=1950, max=2024),
            "posted_on": posted_on.isoformat(),
            "rented_on": rented_on.isoformat() if rented_on else None,
            "bedrooms": int(min(np.random.geometric(p=0.35), 5)),
            "rent": int(np.clip(np.random.normal(loc=2200, scale=500), 900, 10000)),
            "size": int(np.clip(np.random.normal(loc=2000, scale=300), 900, 5000)),
            "duration": self.fake.random_element([6, 12, 18, 24]),
            "city": random.choices(self.cities, self.city_weights, k=1)[0],
            "rental_status": rental_status,
        }
        return listing



listing_gen = RentalListingGen()
def create_producer(bootstrap_servers: str = 'kafka:9092'):
    producer = KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        acks='all',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def main():
    producer = create_producer()

    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events",type=int,default=10)
    args = parser.parse_args()
    num_events = args.num_events

    for i in range(num_events-20):
        future=producer.send('listing-events',listing_gen.generate_listing())

    
    for i in range(20):
        future=producer.send('listing-events',listing_gen.generate_listing())
        time.sleep(1)
    producer.close()

if __name__ == "__main__":
    main()


