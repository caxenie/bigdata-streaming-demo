import json

import matplotlib.pyplot as plt

from kafka import KafkaConsumer
from matplotlib.animation import FuncAnimation

plt.style.use('ggplot')
# The data for plotting comes from Kafka, one needs to create a subscriber
consumer = KafkaConsumer(
    'sink_topic_num',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=False,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
# data storage and visualization parameters
data_dict = {}
t = 0

fig = plt.figure(figsize=(10, 6))
ax1 = fig.add_subplot(2, 1, 1)
ax2 = fig.add_subplot(2, 1, 2)
data_x = {}
data_y = {}
inc_data_y = {}
inc_data_x = {}
data_color = {}
colors = ['red', 'green']
first = True
avg = 0
avg_now = 0
n = 1
sumv = 1

# call this animation function with 5Hz to visualize the results
def animate(message):
    global colors
    global first
    global avg
    global avg_now
    global n
    global sumv
    data = message.value
    # filter for the data of interest
    if data['edge_id'] not in data_x or data['edge_id'] not in data_y or \
            data['edge_id'] not in inc_data_y or data['edge_id'] not in inc_data_x or\
            data['edge_id'] not in data_color:
        data_x[data['edge_id']] = []
        data_y[data['edge_id']] = []
        inc_data_x[data['edge_id']] = []
        inc_data_y[data['edge_id']] = []
        data_color[data['edge_id']] = colors[len(data_x)]
    data_x[data['edge_id']].append(data['step'])
    data_y[data['edge_id']].append(float(data['vehicle_num']))
    inc_data_x[data['edge_id']].append(data['step'])
    inc_data_y[data['edge_id']].append(float(avg_now))
    # implement a simple moving average on the incoming stream of data
    avg_now = avg + 1/sumv*(float(data['vehicle_num']) - avg)
    avg = avg_now
    sumv = sumv + float(data['vehicle_num'])
    ax1.plot(
        data_x[data['edge_id']],
        data_y[data['edge_id']],
        color=data_color[data['edge_id']],
        label=''
    )
    ax1.set_ylabel('# Vehicles')
    ax1.set_title('Road Traffic Monitoring and Analysis\n (Dachauerstrasse - Lothstrasse)')
    ax2.plot(
        inc_data_x[data['edge_id']],
        inc_data_y[data['edge_id']],
        color=data_color[data['edge_id']],
        label=''
    )
    ax2.set_xlabel('Seconds')
    ax2.set_ylabel('Moving Average')
    if len(data_x) == 1 and first:
        first = False


ani = FuncAnimation(fig=fig, func=animate, frames=consumer, interval=200)
plt.show()
