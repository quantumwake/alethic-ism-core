"""
Simple test for concurrent subject processing in NATS routes.

Requirements:
- NATS server running on localhost:4222
"""
import asyncio
import time
from ismcore.messaging.nats_message_route import NATSRoute


def test_concurrent_subjects():
    """Test that max 3 subjects process concurrently."""

    # Track concurrent processing
    processing_count = []  # List of (timestamp, count) tuples
    currently_processing = []
    max_concurrent = 0

    async def callback(route, msg, data):
        """Callback that tracks concurrent processing."""
        subject = msg.subject
        start = time.time()

        # Track that we started processing
        currently_processing.append(subject)
        nonlocal max_concurrent
        current_count = len(currently_processing)
        max_concurrent = max(max_concurrent, current_count)
        processing_count.append((start, current_count, 'START', subject))

        print(f"[{start:.2f}] START {subject:20s} (concurrent: {current_count})")

        # Simulate work
        await asyncio.sleep(1)

        # Track that we finished
        end = time.time()
        currently_processing.remove(subject)
        processing_count.append((end, len(currently_processing), 'END  ', subject))

        print(f"[{end:.2f}] END   {subject:20s} (concurrent: {len(currently_processing)})")

        # ACK the message
        await route.ack(msg)

    async def run_test():
        route = NATSRoute(
            selector="test/concurrent/*",
            name="test_concurrent",
            url="nats://127.0.0.1:4222",
            subject="test.concurrent.*",
            concurrent_subjects=True,
            max_concurrent_subjects=3,
            ack_wait=30,
            batch_size=1,
            callback=callback
        )

        await route.connect()
        await route.subscribe()

        # Start consumer
        consumer_task = asyncio.create_task(route.consume(wait=True))
        await asyncio.sleep(0.5)

        # Publish 10 messages to 10 UNIQUE subjects
        print("\nPublishing 10 messages to 10 unique subjects...")
        print("Max concurrent limit: 3")
        print("Expected: Process in batches of 3, total ~4 seconds\n")

        test_start = time.time()

        for i in range(1, 11):
            await route.publish_with_subject(f"test.concurrent.{i}", f"message {i}".encode())

        # Wait for all messages to complete (count END events)
        end_count = 0
        while end_count < 10:
            await asyncio.sleep(0.1)
            end_count = sum(1 for _, _, event, _ in processing_count if event == 'END  ')
            if time.time() - test_start > 20:
                print("Timeout!")
                break

        total_time = time.time() - test_start

        # Stop consumer
        route.consumer_active = False
        await asyncio.sleep(0.2)
        consumer_task.cancel()
        await route.disconnect()

        # Results
        print(f"\n{'=' * 60}")
        print(f"CONCURRENT TEST RESULTS")
        print(f"{'=' * 60}")
        print(f"Messages processed: {end_count}/10")
        print(f"Total time: {total_time:.2f}s")
        print(f"Max concurrent observed: {max_concurrent}")
        print(f"Max concurrent limit: 3")

        # Validation
        if end_count == 10:
            print("✓ All messages processed")
        else:
            print(f"✗ Only {end_count}/10 messages processed")

        if max_concurrent <= 3:
            print(f"✓ Respected max concurrent limit ({max_concurrent} <= 3)")
        else:
            print(f"✗ FAILED: Exceeded limit ({max_concurrent} > 3)")

        # Expected: 10 messages / 3 concurrent ≈ 4 batches ≈ 4 seconds
        if 3.5 <= total_time <= 5.5:
            print(f"✓ Time within expected range (3.5-5.5s)")
        else:
            print(f"⚠ Time outside expected range ({total_time:.2f}s)")

        if end_count == 10 and max_concurrent <= 3 and 3.5 <= total_time <= 5.5:
            print(f"\n✓✓✓ SUCCESS: Concurrent processing working correctly! ✓✓✓")

    asyncio.run(run_test())


def test_sequential_subjects():
    """Test sequential processing for comparison."""

    processed_count = 0

    async def callback(route, msg, data):
        nonlocal processed_count
        subject = msg.subject
        start = time.time()
        print(f"[{start:.2f}] Processing {subject}")
        await asyncio.sleep(1)
        processed_count += 1
        print(f"[{time.time():.2f}] Completed {subject}")
        await route.ack(msg)

    async def run_test():
        route = NATSRoute(
            selector="test/sequential/*",
            name="test_sequential",
            url="nats://127.0.0.1:4222",
            subject="test.sequential.*",
            concurrent_subjects=False,  # Sequential
            batch_size=10,
            callback=callback
        )

        await route.connect()
        await route.subscribe()

        consumer_task = asyncio.create_task(route.consume(wait=True))
        await asyncio.sleep(0.5)

        print("\nPublishing 10 messages (sequential mode)...")
        print("Expected: Process one at a time, total ~10 seconds\n")

        test_start = time.time()

        for i in range(1, 11):
            await route._js.publish(f"test.sequential.{i}", f"message {i}".encode())

        # Wait for all to complete
        while processed_count < 10:
            await asyncio.sleep(0.1)
            if time.time() - test_start > 20:
                break

        total_time = time.time() - test_start

        route.consumer_active = False
        await asyncio.sleep(0.2)
        consumer_task.cancel()
        await route.disconnect()

        print(f"\n{'=' * 60}")
        print(f"SEQUENTIAL TEST RESULTS")
        print(f"{'=' * 60}")
        print(f"Messages processed: {processed_count}/10")
        print(f"Total time: {total_time:.2f}s (expected ~10s)")

        if 9.5 <= total_time <= 11.0:
            print(f"✓ Sequential processing verified")

    asyncio.run(run_test())


if __name__ == "__main__":
    print("=" * 60)
    print("Testing CONCURRENT subject processing")
    print("=" * 60)
    test_concurrent_subjects()

    print("\n" + "=" * 60)
    print("Testing SEQUENTIAL subject processing (for comparison)")
    print("=" * 60)
    test_sequential_subjects()