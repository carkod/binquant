from kafka import KafkaConsumer
import json
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

# Create empty lists to store the timestamps and Bitcoin prices
timestamps = []
prices = []

# Create a consumer instance
consumer = KafkaConsumer(
    'bitcoin',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Start consuming
for message in consumer:
    topic = message.topic
    value = message.value
    timestamp = message.timestamp / 1000.0  # Convert epoch milliseconds to seconds

    # Round the value to the nearest dollar
    rounded_value = round(value)

    # Append the timestamp and price to the lists
    timestamps.append(datetime.fromtimestamp(timestamp))
    prices.append(rounded_value)

    # Create a pandas DataFrame with timestamps and prices
    df = pd.DataFrame({'Timestamp': timestamps, 'Price': prices})

    # Calculate the 20-period moving average of the Bitcoin price
    df['MA'] = df['Price'].rolling(window=20).mean()

    # Clear the previous plot
    plt.clf()

    # Plot the Bitcoin prices and moving average over time
    plt.plot(df['Timestamp'], df['Price'], label='Bitcoin Price')
    plt.plot(df['Timestamp'], df['MA'], label='Moving Average (20 periods)')
    plt.xlabel('Time')
    plt.ylabel('Bitcoin Price (USD)')
    plt.title('Real-Time Bitcoin Price with Moving Average')
    plt.legend()


    # Adjust the plot margins
    plt.gcf().autofmt_xdate()

    # Display the plot
    plt.pause(0.001)