import json
from typing import Any

import redis.asyncio as redis
from fastapi import Request

async_redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, encoding="utf-8", decode_responses=True)


def get_cache_key(request: Request) -> str:
    return f"cache:{request.url.path}?{request.url.query}"


async def get_from_cache(request: Request):
    key = get_cache_key(request)
    data = await async_redis_client.get(key)
    if data:
        return json.loads(data)
    return None


async def set_cache(request: Request, response_data: Any):
    key = get_cache_key(request)
    await async_redis_client.set(key, json.dumps(response_data))
