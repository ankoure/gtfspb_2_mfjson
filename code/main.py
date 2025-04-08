import os
from dotenv import load_dotenv
import time
from helpers.VehiclePositionFeed import VehiclePositionFeed
from multiprocessing import Pool


if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("API_KEY")
    
    x = VehiclePositionFeed('https://cdn.mbta.com/realtime/VehiclePositions.pb','MBTA','/home/andrew/gtfspb_2_mfjson/data/MBTA/')
    y = VehiclePositionFeed('https://passio3.com/billings/passioTransit/gtfs/realtime/vehiclePositions','Billings','/home/andrew/gtfspb_2_mfjson/data/Billings/')
    z = VehiclePositionFeed('http://gtfs.gcrta.org/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb','Cleveland','/home/andrew/gtfspb_2_mfjson/data/Cleveland/')
    
    test = [x,y,z]
    
    def test_fun(VehiclePositionFeed):
        while True:
            VehiclePositionFeed.consume_pb()
            time.sleep(30)
            
    pool = Pool()                         # Create a multiprocessing Pool
    pool.map(test_fun, test)  # process data_inputs iterable with pool
            
    
    # running = True
    # while running: 
    #     x.consume_pb()
    #     time.sleep(30)
        

