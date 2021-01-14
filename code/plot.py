import json

import matplotlib.pyplot as plt
import numpy as np

# use ggplot style for more sophisticated visuals
from kafka import KafkaConsumer
from matplotlib.animation import FuncAnimation

plt.style.use('ggplot')


consumer = KafkaConsumer(
    'sink_topic_num',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=False,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

data_dict = {}
t = 0
#
# while True:
#     for message in consumer:
#         data = message.value
#         if ':cluster_3050325' in data['edge_id']:
#             if t == 0:
#                 t = message.timestamp
#             data_dict[message.timestamp - t] = float(data['step'])
#             plt.figure(figsize=(10, 10))
#             plt.plot(range(len(data_dict)), list(data_dict.values()))
#             # plt.xticks(range(len(data)), list(data.keys()))
#             plt.show()

# Set the figure for the animation framework
fig = plt.figure(figsize=(10, 6))  # creating a subplot
ax1 = fig.add_subplot(1, 1, 1)
data_x = {}
data_y = {}
data_color = {}
colors = ['red', 'green', 'blue', 'black', 'magenta']
first = True

def animate(message):
    global colors
    global first
    data = message.value
    if data['edge_id'] not in data_x or data['edge_id'] not in data_y or data['edge_id'] not in data_color:
        data_x[data['edge_id']] = []
        data_y[data['edge_id']] = []
        data_color[data['edge_id']] = colors[len(data_x)]
    data_x[data['edge_id']].append(data['step'])
    data_y[data['edge_id']].append(float(data['vehicle_num']))
    # -64464377#3 (NE), -11014139#1 (NW) 161678033#0 (SW) -24970784#3 (SE)
    edge_label = f" #Auto Esplanade "
    if data['edge_id'] == '-11014139#1':
        edge_label = f" #Auto Heydeckstrasse - Esplanade "
    if data['edge_id'] == '-64464377#3':
        edge_label = f" #Auto Heydeckstrasse - Oestliche Ringstrasse "
    if data['edge_id'] == '161678033#0':
        edge_label = f" #Auto Rossmuehlstrasse - Schlosslaende "
    if data['edge_id'] == '-24970784#3':
        edge_label = f" #Auto Schlosslaende - Fruelingstrasse "
    ax1.plot(
        data_x[data['edge_id']],
        data_y[data['edge_id']],
        color=data_color[data['edge_id']],
        label=edge_label
    )
    ax1.set_xlabel('Sekunden')
    ax1.set_ylabel('# Auto')
    ax1.set_title('Verkehrsüberwachung und –analyse')
    if len(data_x) == 4 and first:
        first = False
        plt.legend()


ani = FuncAnimation(fig=fig, func=animate, frames=consumer, interval=10)
plt.show()
