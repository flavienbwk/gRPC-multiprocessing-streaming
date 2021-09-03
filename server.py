# Python std lib
import contextlib
import multiprocessing
import os
import pickle
import random
import signal
import socket
import sys
import tempfile
import threading
from concurrent import futures
from typing import Iterable

# 3rd party libs
import cv2
import grpc
import numpy as np
from simber import Logger

# Local grpc module
sys.path.append("/usr/app/grpc_config")
import video_thumbnail_pb2
import video_thumbnail_pb2_grpc

LOG_LEVEL: str = "INFO"
logger = Logger(__name__, log_path="/tmp/logs/server.log", level=LOG_LEVEL)
logger.update_format("{levelname} [{filename}:{lineno}]:")

NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 1))
MAX_GRPC_PAYLOAD_SIZE = int(os.environ.get("MAX_GRPC_PAYLOAD_SIZE", 2000000000))


def get_video_thumbnail(video_path: str) -> np.ndarray:
    """
    Args:
        video_path (str): Video to get the thumbnail from

    Returns:
        (np.ndarray) : Video thumbnail
    """
    cap = cv2.VideoCapture(video_path)
    nb_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    thumbnail_frame = random.randint(0, nb_frames - 1)  # choosing thumbnail randomly
    thumbnail: np.ndarray = None
    frame_count = 0

    while True:
        ret, frame = cap.read()
        if ret == True:
            if frame_count == thumbnail_frame:
                thumbnail = frame
                break
        else:
            break
        frame_count += 1
    cap.release()
    cv2.destroyAllWindows()
    return thumbnail


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


def perform_binary_slicing_payloads(
    data: bytes, chunk_size: int
) -> Iterable[video_thumbnail_pb2.VideoResult]:
    """
    Slices a binary blob into chunks of chunk_size with gRPC message expected by the server.
    """
    for chunk in perform_binary_slicing(data, chunk_size):
        yield video_thumbnail_pb2.VideoResult(error=False, chunk=chunk)


class VideoService(video_thumbnail_pb2_grpc.VideoServicer):
    def __init__(self, worker_id) -> None:
        super().__init__()
        self.worker_id = worker_id

    def Process(
        self, request_chunks: Iterable[video_thumbnail_pb2.VideoCandidate], context
    ) -> Iterable[video_thumbnail_pb2.VideoResult]:
        logger.info(f"[Worker {self.worker_id}] Processing incoming request...")

        # Processing incoming video chunks to form original video
        video_bytearray: bytearray = bytearray()
        try:
            for request_chunk in request_chunks:
                video_bytearray.extend(request_chunk.chunk)
            with tempfile.NamedTemporaryFile("wb", suffix=".mp4") as input_video_file:
                input_video_file.write(bytes(video_bytearray))
                video_thumbnail = get_video_thumbnail(input_video_file.name)
            logger.info(f"[Worker {self.worker_id}] Finished thumbnailing.")
            thumbnail_binary = pickle.dumps(video_thumbnail)
            for chunk in perform_binary_slicing_payloads(
                thumbnail_binary, MAX_GRPC_PAYLOAD_SIZE
            ):
                yield chunk
        except Exception as e:
            logger.error(e)
            yield video_thumbnail_pb2.VideoResult(error=True, chunk=None)


def _run_server(bind_address, worker_id):
    def on_done(signum, frame):
        logger.info("Got signal {}, {}".format(signum, frame))
        done.set()

    logger.info(f"[Worker {worker_id}] Server started. Awaiting jobs...")
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1),
        options=[
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
            ("grpc.so_reuseport", 1),
            ("grpc.use_local_subchannel_pool", 1),
        ],
    )
    video_thumbnail_pb2_grpc.add_VideoServicer_to_server(
        VideoService(worker_id),
        server,
    )
    server.add_insecure_port(bind_address)
    server.start()
    done = threading.Event()
    signal.signal(signal.SIGTERM, on_done)  # catch SIGTERM for clean container exit
    done.wait()
    server.wait_for_termination()


@contextlib.contextmanager
def _reserve_port():
    """Find and reserve a port for all subprocesses to use"""
    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) == 0:
        raise RuntimeError("[Main] Failed to set SO_REUSEPORT.")
    sock.bind(("", 13000))
    try:
        yield sock.getsockname()[1]
    finally:
        sock.close()


def main():
    """
    Starts gRPC server and its workers.
    Inspired from https://github.com/grpc/grpc/blob/master/examples/python/multiprocessing/server.py
    """
    logger.info(f"[Main] Initializing server with {NUM_WORKERS} workers")
    with _reserve_port() as port:
        bind_address = f"[::]:{port}"
        logger.info(f"[Main] Binding to {bind_address}")
        workers = []
        for worker_id in range(NUM_WORKERS):
            logger.info(f"[Main] Starting worker {worker_id}...")
            worker = multiprocessing.Process(
                target=_run_server, args=(bind_address, worker_id)
            )
            worker.start()
            workers.append(worker)
        for worker in workers:
            worker.join()


if __name__ == "__main__":
    main()
