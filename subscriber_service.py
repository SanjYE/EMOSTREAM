import asyncio
import websockets
import redis.asyncio as redis
import json
import logging

logging.basicConfig(level=logging.INFO)

redis_client = redis.Redis(host='localhost', port=6379, db=0)

connected_clients = {}

async def consumer_handler(websocket):
    try:
        channel_message = await websocket.recv()
        channel_data = json.loads(channel_message)
        channel = channel_data.get('channel')

        if not channel:
            await websocket.send(json.dumps({'error': 'Channel is required'}))
            return

        if channel not in connected_clients:
            connected_clients[channel] = set()
        connected_clients[channel].add(websocket)

        logging.info(f"Client connected to channel: {channel}. Total clients in this channel: {len(connected_clients[channel])}")

        await websocket.wait_closed()

    except Exception as e:
        logging.error(f"Error in consumer handler: {e}")

    finally:
        if channel in connected_clients:
            connected_clients[channel].remove(websocket)
            if not connected_clients[channel]: 
                del connected_clients[channel]
        logging.info(f"Client disconnected from channel: {channel}")

async def redis_listener():
    pubsub = redis_client.pubsub()
    await pubsub.subscribe('emoji_updates')

    async for message in pubsub.listen():
        if message['type'] == 'message':
            data_str = message['data'].decode('utf-8')
            logging.info(f"Received data: {data_str}")
            await broadcast_to_all_channels(data_str)

async def broadcast_to_all_channels(data):
    for channel, clients in connected_clients.items():
        disconnected_clients = set()
        for client in clients:
            try:
                await client.send(data)
            except Exception as e:
                logging.error(f"Error sending data to client: {e}")
                disconnected_clients.add(client)

        clients.difference_update(disconnected_clients)

async def main():
    async with websockets.serve(consumer_handler, '0.0.0.0', 8765):
        await redis_listener()

if __name__ == '__main__':
    asyncio.run(main())
