import logging


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        # "[%(asctime)s] [%(filename)-20.20s] %(levelname)-8s: %(message)s"
        format="[%(asctime)s] %(levelname)-8s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
