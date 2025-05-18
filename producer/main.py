from quixstreams import Application  

import random
import os
import json
import glob
import tqdm
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="data_source",
                auto_create_topics=True,
                broker_address="kafka_broker:9092"
) 

topic_name = os.environ["output"]
topic = app.topic(topic_name)

def main():
    """
    Read data from the hardcoded dataset and publish it to Kafka
    """

    with app.get_producer() as producer:

        files = glob.glob('nasdaq/*.zst')
        files.sort()

        for file_path in tqdm.tqdm(files):
            print(f'Processing file: {file_path}')

            data = pd.read_csv(file_path)

            for _, row in data.iterrows():
                trade = row.to_dict()

                json_data = json.dumps(trade)  

                producer.produce(
                    topic=topic.name,
                    key=trade['symbol'],
                    value=json_data,
                )

        print("All rows published")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")