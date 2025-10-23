import os
import asyncio
from memphis import Memphis

MEMPHIS_BROKER_HOST = os.environ.get("MEMPHIS_BROKER_HOST")
MEMPHIS_BROKER_PORT = int(os.environ.get("MEMPHIS_BROKER_PORT"))
MEMPHIS_ADMIN_USERNAME = os.environ.get("MEMPHIS_ADMIN_USERNAME")
MEMPHIS_ADMIN_PASSWORD = os.environ.get("MEMPHIS_ADMIN_PASSWORD")
MEMPHIS_TOPIC_NAME = os.environ.get("MEMPHIS_TOPIC_NAME")
PARTITION_COUNT = int(os.environ.get("PARTITION_COUNT"))
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", 1))

async def create_memphis_station():
    try:
        memphis = Memphis()
        await memphis.connect(
            host=MEMPHIS_BROKER_HOST,
            port=MEMPHIS_BROKER_PORT,
            username=MEMPHIS_ADMIN_USERNAME,
            password=MEMPHIS_ADMIN_PASSWORD,
        )
        print("Memphis connection successful.")

        try:
            await memphis.station(
                name=MEMPHIS_TOPIC_NAME, 
                partitions_number=PARTITION_COUNT,
                # retention_type="message_age_sec",
                # retention_value=604800, # 7일
                # replicas=REPLICATION_FACTOR,
                # storage_type="disk"
            )
            print(f"Station '{MEMPHIS_TOPIC_NAME}' created successfully.")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Station '{MEMPHIS_TOPIC_NAME}' already exists. Skipping creation.")
            else:
                raise e
    except Exception as e:
        print(f"An error occurred during Memphis station creation: {e}")
    finally:
        try:
            await memphis.close()
        except UnboundLocalError:
            pass

if __name__ == "__main__":
    print(f"Attempting connection to broker: {MEMPHIS_BROKER_HOST}:{MEMPHIS_BROKER_PORT}")
    print(f"Username: '{MEMPHIS_ADMIN_USERNAME}'")
    
    asyncio.run(create_memphis_station())
