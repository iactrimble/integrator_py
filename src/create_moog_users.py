# local imports
import xmatters
import config

# python3 package imports
import logging
import json
from logging.handlers import RotatingFileHandler


# main process
def main() -> object:
    """
           Purpose:
           process to create a list of users in xMatters
           Built to migrate Moogsoft users from UAT to Prod but could be used for other purposes

    """

    # get a distinct list of users
    moog_users = moog_file.get_rows(["id"], {}, True, ",")
    log.debug("Moog_users: " + json.dumps(moog_users))

    update_data = []

    for u in moog_users:
        log.debug("Processing " + u["id"])
        targetName = u["id"]

        data = {"data":{
            "targetName": targetName,
            "firstName": targetName,
            "lastName": targetName,
            "recipientType": "PERSON",
            "status": "ACTIVE",
            "language": "en",
            "timezone": "US/Pacific",
            "webLogin": targetName,
            "roles": ["Standard User"],
            "site": "16f36c72-b00b-456b-b492-4a0757352960",
            "supervisors": ["65145f9a-aca2-4e61-96e2-84baf5e0bf42"]
            }
        }

        update_data.append(data)



    if len(update_data) > 0:
        person_response = xm_collection.create_collection(xm_person.create_person, update_data, config.dynamic_team_custom_fields['thread_count'])
        log.info("Update response: " + str(person_response["response"]))
        log.info("Update errors: " + str(person_response["errors"]))



# entry point when file initiated
if __name__ == "__main__":

    # configure the logging
    logging.basicConfig(level=config.moog['logging']["level"], datefmt="%m-%d-%Y %H:%M:%Srm ",
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
                        handlers=[RotatingFileHandler(config.moog['logging']["file_name"], maxBytes=config.moog['logging']["max_bytes"], backupCount=config.moog['logging']["back_up_count"])])
    log = logging.getLogger(__name__)

    # time start
    time_util = xmatters.TimeCalc()
    start = time_util.get_time_now()
    log.info("Starting Process: " + time_util.format_date_time_now(start))
    print("Starting Process: " + time_util.format_date_time_now(start))

    # instantiate classes
    environment = xmatters.xMattersAPI(config.environment["url"], config.environment["username"], config.environment["password"])
    xm_person = xmatters.xMattersPerson(environment)
    xm_collection = xmatters.xMattersCollection(environment)
    moog_file = xmatters.Column(config.moog['file_name'], config.moog["encoding"])

    # execute the main process
    main()

    # end the duration
    end = time_util.get_time_now()
    log.info("Process Duration: " + time_util.get_diff(end, start))
    print("Process Duration: " + time_util.get_diff(end, start))