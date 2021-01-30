'''
Handles connection to influxdb server, actually writes data to database
HOST_1 and HOST_2 are the two different influxdb servers currently running
Refrain from writing to the same db from two different scripts as that will create duplicate points
'''

from influxdb import InfluxDBClient


# host name
HOST = "ec2-15-222-236-45.ca-central-1.compute.amazonaws.com"

class Writer:

	def __init__(self):
		self.client = InfluxDBClient(host=HOST, port='8086', username='', password='')
		self.client.switch_database('md_rates')

	'''
	Functions that interact directly with db
	'''

	def write_points_to_measurement(self, measurement, time, tags, fields):
		json_body = []
		if time:
			json_body = [
				{
					"measurement": measurement,
					"time": time,
					"tags": tags,
					"fields": fields
				}
			]
		else:
			json_body = [
				{
					"measurement": measurement,
					"tags": tags,
					"fields": fields
				}
			]

		return self.client.write_points(json_body)
    
    
	def write_multiple_points_to_measurement(self, line_protocol_body,time_precision):
        
		return self.client.write_points(line_protocol_body, database='md_rates',time_precision=time_precision,batch_size=10000, protocol='line')
        
        
### Line protocol without timestamp
### {measurement},symbol={symbol} amount={amount},direction="{direction}",price={price},id={id}i

### Line protocol with timestamp
### {measurement},symbol={symbol} amount={amount},direction="{direction}",price={price},id={id}i {timestamp}
