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
        Purpose:
        Get a list of users and update custom fields if the user has a correlating device type
        This will help build targeted dynamic teams to reduce the use of SMS messaging

        1. Query for ACTIVE users in xM
        2. Interrogate their devices and set boolean properties
        3. Build update user payload if properties have changed
        4. Update xMatters users
    """

    # Get all ACTIVE people and their devices
    people = []

    param_data = {
        "url_filter": '?status=ACTIVE&embed=devices'
    }
    try:
        # get initial page of people
        people_search = xm_person.get_people(param_data['url_filter'] + '&offset=0&limit=' + str(config.dynamic_team_custom_fields['page_size']))

        # if nothing is returned let's skip this search loop
        if not people_search:
            log.info('No users found from the instance for search: ' + str(param_data['url_filter']))
            return "No Users Found"

        # if the total returned from the the search is greater than the config page size, then we have more searching to do
        if people_search['total'] > config.dynamic_team_custom_fields['page_size']:

            people_collection = xm_collection.get_collection(xm_person.get_people, people_search['total'],
                                                             config.dynamic_team_custom_fields['page_size'], param_data,
                                                             config.dynamic_team_custom_fields['thread_count'])

            # log and then concat two arrays
            log.info("Retrieved " + str(len(people_collection['response'])) + " people from search: " + str(
                param_data['url_filter']))
            people = people_collection['response'] + people
        else:
            # else, continue on with that initial request and concat the two arrays
            log.info(
                "Retrieved " + str(len(people_search['data'])) + " people from search: " + str(param_data['url_filter']))
            people = people_search['data'] + people

        log.debug('Retrieved people data: ' + json.dumps(people))
        log.info('Retrieved people count: ' + str(len(people)))
    except Exception as e:
        log.error('Exception ' + str(e))

    # Check each persons devices as set custom fields
    request_data = []

    device_types = config.dynamic_team_custom_fields['properties']['device_types']
    log.debug('Device Types to look for: ' + str(device_types))

    custom_fields = config.dynamic_team_custom_fields['properties']['custom_fields']
    log.debug('Custom Fields to populate: ' + str(custom_fields))

    # create csv output file
    with open(config.dynamic_team_custom_fields['file']['dt_custom_fields_file_name'], 'w', newline='',encoding=config.dynamic_team_custom_fields['file']['encoding']) as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

        # write the csv column headers
        csv_writer.writerow(['targetName', 'has_mobile_app', 'has_sms', 'has_voice', 'timestamp'])

        today_date = str(datetime.date.today().isoformat())

        for data in people:

            #reset flags
            has_app=False
            has_sms=False
            has_voice=False

            try:
                if data['status'] == "ACTIVE":
                    if "devices" in data:
                        for device in data['devices']['data']:
                            if device['deviceType'] == "ANDROID_PUSH" or device['deviceType'] == "APPLE_PUSH":
                                has_app=True
                            if device['deviceType'] == "TEXT_PHONE":
                                has_sms=True
                            if device['deviceType'] == "VOICE":
                                if device['name'] in device_types:
                                    has_voice=True

                log.debug(data['targetName'] + ' - Has Mobile App: ' + str(has_app) + ' - Has SMS: ' + str(has_sms) + ' - Has Voice: ' + str(has_voice) )

                # write to csv
                csv_writer.writerow([data['targetName'], has_app, has_sms, has_voice, today_date])

                # if a persons devices have changed
                if data['properties'][custom_fields[0]] != has_app or data['properties'][custom_fields[1]] != has_sms or data['properties'][custom_fields[2]] != has_voice:
                    log.debug(data['targetName'] + ' devices have changed, adding to update object')

                    properties = {
                        custom_fields[0]: has_app,
                        custom_fields[1]: has_sms,
                        custom_fields[2]: has_voice,
                    }

                    request_data.append(dict(data=dict(targetName=data['targetName'],
                                         id=data['id'],
                                         properties=properties)))
            except Exception as e:
                log.error('Exception ' + str(e) + ' on line:  ' + str(data))

    log.debug('Retrieved request data: ' + json.dumps(request_data))
    log.info('Number of requests for update: ' + str(len(request_data)))

    # update custom fields
    if len(request_data) > 0:
        try:
            person_response = xm_collection.create_collection(xm_person.modify_person, request_data, config.dynamic_team_custom_fields['thread_count'])
            log.debug("Update response: " + str(person_response["response"]))
            log.info("Update errors: " + str(person_response["errors"]))
        except Exception as e:
            log.error('Exception ' + str(e))

if __name__ == "__main__":
    # configure the logging
    logging.basicConfig(level=config.dynamic_team_custom_fields['logging']["level"], datefmt="%m-%d-%Y %H:%M:%Srm ",
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
                        handlers=[RotatingFileHandler(config.dynamic_team_custom_fields['logging']["file_name"],
                                                      maxBytes=config.dynamic_team_custom_fields['logging']["max_bytes"],
                                                      backupCount=config.dynamic_team_custom_fields['logging']['back_up_count'])])
    log = logging.getLogger(__name__)

    # time start
    time_util = xmatters.TimeCalc()
    start = time_util.get_time_now()
    log.info("Starting Process: " + time_util.format_date_time_now(start))

    # instantiate classes
    environment = xmatters.xMattersAPI(config.environment["url"], config.environment["username"],
                                       config.environment["password"])
    xm_person = xmatters.xMattersPerson(environment)
    xm_collection = xmatters.xMattersCollection(environment)


    main()  # execute the main process

    # end the duration
    end = time_util.get_time_now()
    log.info("Process Duration: " + time_util.get_diff(end, start))
