# Python std lib
import concurrent.futures
import glob
import cv2
import os
import pickle
import sys
import time
from typing import Iterable, Tuple

# 3rd party libs
import grpc
import numpy as np
from simber import Logger

# Local gRPC modules
sys.path.append("/usr/app/grpc_config")
import video_thumbnail_pb2
import video_thumbnail_pb2_grpc

LOG_LEVEL: str = "INFO"
logger = Logger(__name__, log_path="/tmp/logs/client.log", level=LOG_LEVEL)
logger.update_format("{levelname} [{filename}:{lineno}]:")

NUM_JOBS = int(os.environ.get("NUM_JOBS", 1))
MAX_GRPC_PAYLOAD_SIZE = int(os.environ.get("MAX_GRPC_PAYLOAD_SIZE", 2000000000))


def perform_binary_slicing(data: bytes, chunk_size: int) -> Iterable[bytes]:
    """
    Slices a binary blob into chunks of chunk_size.
    """
    data_size = sys.getsizeof(data)
    if data_size < chunk_size:
        yield data
    current_chunk = 0
    while current_chunk < data_size:
        chunk = data[current_chunk : current_chunk + chunk_size]
        current_chunk += chunk_size
        yield chunk


def perform_binary_slicing(data: bytes, chunk_size: int) -> Iterable[bytes]:
    """
    Slices a binary blob into chunks of chunk_size.
    """
    data_size = sys.getsizeof(data)
    if data_size < chunk_size:
        yield data
    current_chunk = 0
    while current_chunk < data_size:
        chunk = data[current_chunk : current_chunk + chunk_size]
        current_chunk += chunk_size
        yield chunk


def perform_binary_slicing_payloads(data: bytes, chunk_size: int) -> Iterable:
    """
    Slices a binary blob into chunks of chunk_size with gRPC message expected by the server.
    """
    for chunk in perform_binary_slicing(data, chunk_size):
        yield video_thumbnail_pb2.VideoCandidate(chunk=chunk)


def _run_worker_query(
    server_address: str,
    video_identifier: str,
    video_binary: bytes,
) -> Tuple[str, np.ndarray]:
    """
    Executes the call to the gRPC server.

    Args:
        server_address (str): gRPC server endpoint
        video_binary (bytes): bytes representation of the video

    Returns:
        tuple(str, np.ndarray): video_identifier, thumbnail as np array (OpenCV-ready)
    """
    channel = grpc.insecure_channel(
        server_address,
        options=[
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
            ("grpc.so_reuseport", 1),
            ("grpc.use_local_subchannel_pool", 1),
        ],
    )
    worker_stub = video_thumbnail_pb2_grpc.VideoStub(channel)

    response_chunks: Iterable[video_thumbnail_pb2.VideoResult] = worker_stub.Process(
        perform_binary_slicing_payloads(video_binary, MAX_GRPC_PAYLOAD_SIZE)
    )
    video_thumbnail_binary: bytearray = bytearray()
    for response_chunk in response_chunks:
        if response_chunk.error == True:
            return video_identifier, None
        video_thumbnail_binary.extend(response_chunk.chunk)
    return (
        video_identifier,
        pickle.loads(video_thumbnail_binary) if len(video_thumbnail_binary) else None,
    )


def get_video_binaries(directory: str) -> Tuple[str, Iterable[bytes]]:
    for file_path in glob.glob(f"{directory}/*.mp4"):
        with open(file_path, "rb") as video:
            yield file_path, video.read()


def run():
    processes = []
    server_address = "server:13000"
    logger.info("Video thumbnailing client started.")

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_JOBS) as executor:

        for video_path, video_binary in get_video_binaries(directory="/usr/app/input"):
            logger.info(f"Sending gRPC query for {video_path}...")
            processes.append(
                executor.submit(
                    _run_worker_query,
                    server_address,
                    os.path.basename(video_path),
                    video_binary,
                )
            )

        for task in concurrent.futures.as_completed(processes):
            video_identifier, video_thumbnail = task.result()
            video_thumbnail_path = f"/usr/app/output/{video_identifier}.jpg"
            if video_thumbnail is None:
                logger.error("Server failed to produce thumbnail for this video")
                continue
            cv2.imwrite(video_thumbnail_path, video_thumbnail)
            logger.info(f"Wrote thumbnail of video in {video_thumbnail_path}")

    duration = time.perf_counter() - start
    logger.info(f"Finished in {duration} seconds")


if __name__ == "__main__":
    run()
