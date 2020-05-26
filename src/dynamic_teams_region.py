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
           Build a custom field to identify the dynamic team a user belongs to. This helps reduce complexity of current Teams
           and reduce the number of SMS messages

           Dynamic teams are built with criteria 'city = XXXX OR city = YYY OR city == zzz' etc
           This process creates a single custom field with the respective DT name so we can use the additional device fields
           (see dynamic_team_custom_fields.py) to help limit the number of SMS messages sent

    """

    try:
        # Get all ACTIVE people
        people = []

        param_data = {
            "url_filter": '?status=ACTIVE'
        }

        print("Getting users")
        # get initial page of people
        people_search = xm_person.get_people(
            param_data['url_filter'] + '&offset=0&limit=' + str(config.dynamic_team_custom_fields['page_size']))

        # if nothing is returned let's skip this search loop
        if not people_search:
            log.info('No users found from the instance for search: ' + str(param_data['url_filter']))
            return "No Users Found"

        # if the total returned from the search is greater than the config page size, then we have more searching to do
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
        print('Retrieved people count: ' + str(len(people)))

        #####################################
        # End of getting people
        #
        # Start of processing dynamic teams
        #####################################

        # get a distinct list of dynamic teams
        # dynamic_teams_data = dynamic_teams_file.get_rows(["targetName", "operand"], {"targetName", "operand"}, True, ";")
        dynamic_teams_data = dynamic_teams_file.get_rows(["targetName", "operand"], {"targetName"}, True, ";")
        log.debug("dynamic_teams_data: " + json.dumps(dynamic_teams_data))

        # get region field property name
        dt_region_field = config.dynamic_team_custom_fields["properties"]["dt_region_field"]

        update_data = []
        added_people = []
        dt_cnt = 0

        # #####################################
        #
        # Process criteria that have an AND operand
        #
        # #####################################

        for data in dynamic_teams_data:
            dt_cnt = 0
            if data["operand"] == 'AND':
                log.info("processing Dynamic Team :  " + data["targetName"])
                print("Processing Dynamic Team :  " + data["targetName"])

                # get criteria for dynamic team
                dynamic_teams_criteria = dynamic_teams_file.get_rows(["field", "value"],{"targetName": data["targetName"]},False)
                dt_length = len(dynamic_teams_criteria)
                log.debug("dynamic_teams_criteria_length: " + str(dt_length))

                # iterate through each user and check if they match thge criteria
                for person in people:
                    log.debug("Processing " + person["targetName"])
                    # log.debug("JSON: " + json.dumps(person))

                    add_data = False
                    do_add = False
                    user_criteria_match = 0

                    if "properties" not in person:
                        log.debug(person["targetName"] + ' has no properties')
                        break

                    for row in dynamic_teams_criteria:
                        key = row["field"]
                        val = row["value"]

                        if key not in person["properties"]:
                            log.debug(person["targetName"] + " has no " + key + " property")
                            break

                        if person["properties"][key] == val:
                            user_criteria_match = user_criteria_match+1
                            log.debug(person["targetName"] + " " + person["properties"][key] + " = " + val)
                            add_data = True

                            if user_criteria_match < dt_length:
                                continue

                        else:
                            add_data = False
                            log.debug(person["targetName"] + " " + person["properties"][key] + " NOT " + val)
                            break

                        if add_data:
                            added_people.append(person["targetName"])

                            log.debug("Adding user " + person["targetName"] + " to " + data["targetName"])
                            if dt_region_field in person['properties']:
                                if person['properties'][dt_region_field] != data["targetName"]:
                                    do_add = True
                            else:
                                do_add = True

                        if do_add:
                            dt_cnt += 1
                            update_data.append({
                                "data": {
                                    "id": person["id"],
                                    "targetName": person["targetName"],
                                    "properties": {
                                        dt_region_field: data["targetName"]
                                    }
                                }
                            })
                            log.debug("Removing " + person["targetName"] + " from People array")
                            break

                log.info("Number of users to update: " + str(dt_cnt))
                print("Number of users to update: " + str(dt_cnt))

        log.debug('Added People: ' + str(added_people))


        # #####################################
        #
        # Process criteria that have an OR operand
        #
        # #####################################

        for data in dynamic_teams_data:
            dt_cnt = 0

            if data["operand"] == 'OR':
                log.info("processing Dynamic Team :  " + data["targetName"])
                print("Processing Dynamic Team :  " + data["targetName"])

                # get criteria for dynamic team
                dynamic_teams_criteria = dynamic_teams_file.get_rows(["field", "value"],
                                                                     {"targetName": data["targetName"]}, False)
                dt_length = len(dynamic_teams_criteria)
                log.debug("dynamic_teams_criteria_length: " + str(dt_length))

                # iterate through each user and check if they match thge criteria
                for person in people:
                    log.debug("Processing " + person["targetName"])
                    # log.debug("JSON: " + json.dumps(person))

                    add_data = False
                    do_add = False

                    if "properties" not in person:
                        log.debug(person["targetName"] + ' has no properties')
                        break

                    for row in dynamic_teams_criteria:
                        key = row["field"]
                        val = row["value"]

                        if key not in person["properties"]:
                            # log.debug(person["targetName"] + " has no " + key + " property")
                            continue

                        if person["properties"][key] == val:
                            log.debug(person["targetName"] + " " + person["properties"][key] + " = " + val)
                            add_data = True

                        else:
                            add_data = False
                            # log.debug(person["targetName"] + " " + person["properties"][key] + " NOT " + val)
                            continue

                        if add_data:
                            log.debug("Adding user " + person["targetName"] + " to " + data["targetName"])
                            if dt_region_field in person['properties']:
                                if person['properties'][dt_region_field] != data["targetName"]:
                                    do_add = True
                            else:
                                do_add = True

                        if do_add:
                            if person["targetName"] not in added_people:
                                dt_cnt += 1
                                update_data.append({
                                    "data": {
                                        "id": person["id"],
                                        "targetName": person["targetName"],
                                        "properties": {
                                            dt_region_field: data["targetName"]
                                        }
                                    }
                                })

                                break
                log.info("Number of users to update: " + str(dt_cnt))
                print("Number of users to update: " + str(dt_cnt))

        log.info("Update Data has " + str(len(update_data)) + " users to update")
        print("Update Data has " + str(len(update_data)) + " users to update")
        log.info("Update_Data: " + json.dumps(update_data))

        # #####################################
        #
        # Update users if there are any to update
        #
        # #####################################

        # if len(update_data) > 0:
        #     person_response = xm_collection.create_collection(xm_person.modify_person, update_data, config.dynamic_team_custom_fields['thread_count'])
        #     log.info("Update response: " + str(person_response["response"]))
        #     log.info("Update errors: " + str(person_response["errors"]))

    except Exception as e:
        log.error('Exception ' + str(e))

# entry point when file initiated
if __name__ == "__main__":

    # configure the logging
    logging.basicConfig(level=config.dynamic_team_custom_fields['logging']["level"], datefmt="%m-%d-%Y %H:%M:%Srm ",
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
                        handlers=[RotatingFileHandler(config.dynamic_team_custom_fields['logging']["file_name"], maxBytes=config.dynamic_team_custom_fields['logging']["max_bytes"], backupCount=config.dynamic_team_custom_fields['logging']["back_up_count"])])
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
    dynamic_teams_file = xmatters.Column(config.dynamic_team_custom_fields['file']["dt_region_file_name"], config.dynamic_team_custom_fields['file']["encoding"])

    # execute the main process
    main()

    # end the duration
    end = time_util.get_time_now()
    log.info("Process Duration: " + time_util.get_diff(end, start))
    print("Process Duration: " + time_util.get_diff(end, start))