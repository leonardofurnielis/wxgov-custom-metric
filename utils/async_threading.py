import asyncio


def run_in_thread(func, *args, **kwargs):
    # Force default asyncio event loop.
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    asyncio.set_event_loop(asyncio.new_event_loop())

    return func(*args, **kwargs)
