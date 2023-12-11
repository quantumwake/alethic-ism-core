import pulsar
import asyncio
import logging as log

from pydantic import ValidationError

from processor.processor_state import State

MSG_URL="pulsar://localhost:6650"
MSG_QA_TOPIC="qa_topic"
MSG_QA_TOPIC_SUBSCRIPTION="qa-subscription"
# MSG_ASSET_AUTOTAG_TOPIC="asset_autotag"

client = pulsar.Client(MSG_URL)
consumer = client.subscribe(MSG_QA_TOPIC, MSG_QA_TOPIC_SUBSCRIPTION)
# producer = client.create_producer(MSG_ASSET_AUTOTAG_TOPIC,
#                                   schema=schema.StringSchema())

logging = log.getLogger(__name__)

def forward_state(state: State):

    try:
        pass
        # producer.send(json.dumps({"asset_id": f"{asset.id}", "force": "false"}), None)
    except Exception as e:
        print("Failed to send message: %s", e)
        raise e

    # producer.flush()


def close_producer():
    pass
    # producer.close()


def close(consumer):
    consumer.close()

async def qa_topic_consumer():
    while True:
        try:
            msg = consumer.receive()
            data = msg.data().decode("utf-8")

            logging.info(f'Message received with {data}')


            # asset = model.AssetOut.model_validate_json(data)
            # Log message receipt
            # logger.info(f"Message received with asset id {.id} for library {asset.library_id}")

            # send ack that the message was consumed.
            consumer.acknowledge(msg)

            # Log success
            # logger.info(
            #     f"Message successfully consumed and stored with asset id {asset.id} for account {asset.library_id}")
        except pulsar.Interrupted:
            logging.error("Stop receiving messages")
            break
        except ValidationError as e:
            # it is safe to assume that if we get a validation error, there is a problem with the json object
            # TODO throw into an exception log or trace it such that we can see it on a dashboard
            consumer.acknowledge(msg)
            logging.error(f"Message validation error: {e} on asset data {data}")
        except Exception as e:
            consumer.acknowledge(msg)
            # TODO need to send this to a dashboard, all excdptions in consumers need to be sent to a dashboard
            logging.error(f"An error occurred: {e} on asset data {data}")


if __name__ == '__main__':
    asyncio.run(qa_topic_consumer())