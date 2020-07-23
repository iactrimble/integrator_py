# local imports
import xmatters
import config

# python3 package imports
import logging
import base64
import json
from logging.handlers import RotatingFileHandler
import urllib.parse
import datetime
import csv


# main process
def main() -> object:
    """

        1. Get all inactive email devices
        2. Activate those devices

    """

    devices = xm_device.get_devices('?deviceStatus=INACTIVE&deviceType=EMAIL')
    for device in devices['data']:
        data = {
            "id": device['id'],
            "deviceType": device['deviceType'],
            "status": "ACTIVE"
        }
        xm_device.modify_device(data)


if __name__ == "__main__":
    # configure the logging
    logging.basicConfig(level=config.devices['logging']["level"], datefmt="%m-%d-%Y %H:%M:%Srm ",
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
                        handlers=[RotatingFileHandler(config.devices['logging']["file_name"],
                                                      maxBytes=config.devices['logging']["max_bytes"],
                                                      backupCount=config.devices['logging']['back_up_count'])])
    log = logging.getLogger(__name__)

    # time start
    time_util = xmatters.TimeCalc()
    start = time_util.get_time_now()
    log.info("Starting Process: " + time_util.format_date_time_now(start))

    # instantiate classes
    environment = xmatters.xMattersAPI(config.environment["url"], config.environment["username"],
                                       config.environment["password"])
    xm_device = xmatters.xMattersDevice(environment)

    main()  # execute the main process

    # end the duration
    end = time_util.get_time_now()
    log.info("Process Duration: " + time_util.get_diff(end, start))
