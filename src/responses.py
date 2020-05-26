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
        1. Query for all events where a form property is set to true and push it to an array
        2. Query for audits of each individual event and push it to an array
        3. Write to a CSV

    """

    today_date = str(datetime.date.today().isoformat())
    # today_date = '2020-04-15'  # uncomment to override above to a past date stamp
    log.info(today_date)

    events = xm_event.get_events('embed=targetedRecipients&propertyName=response_report&propertyValue=true&from=' + today_date +urllib.parse.quote('T00:00:00.000Z', safe=''))

    log.debug('Received events ' + json.dumps(events))

    log.info('Getting User Deliveries for ' + str(len(events['data'])) + ' events.')
    current_date_time = datetime.datetime.utcnow()

    csv_data = []
    for event in events['data']:

        # collect details about each event
        event_details = {
            "event_id": event['eventId'],
            "event_uuid": event['id'],
            "workflow_id": event['plan']['id'],
            "workflow_name": event['plan']['name'],
            "form_id": event['form']['id'],
            "form_name": event['form']['name'],
        }

        # collect the directly targeted recipient(s).  Groups, dynamic teams or people
        recipientTargetName = []
        recipientTargetType = []
        peopleRecipients = []
        dynamicTeamRecipients = []
        groupRecipients = []
        if "recipients" in event:
            if event["recipients"]["count"] > 0:
                for recipient in event["recipients"]["data"]:
                    if recipient["recipientType"] == "PERSON":
                        peopleRecipients.append(recipient["targetName"])
                    if recipient["recipientType"] == "DYNAMIC_TEAM":
                        dynamicTeamRecipients.append(recipient["targetName"])
                    if recipient["recipientType"] == "GROUP":
                        groupRecipients.append(recipient["targetName"])

        # event_details["recipientTargetName"] = recipientTargetName
        # event_details["recipientTargetType"] = recipientTargetType

        # get notification delivery details for each recipient
        event_user_delivery = xm_event.get_user_deliveries(event['id'], 'at=' + str(
            current_date_time.strftime('%Y-%m-%dT%H:%M:%SZ')) + '&offset=0&limit='+str(config.responses['page_size']))

        if not event_user_delivery:
            log.info('No log data found for event id ' + event['eventId'] + ' moving to next event id.')
            continue

        # if above the page size limit execute the collection
        if event_user_delivery['total'] > config.responses['page_size']:
            param_data = {
                "url_filter": 'at=' + str(current_date_time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                "event_id": event['id']
            }
            event_user_delivery_collection = xm_collection.get_collection(xm_event.get_user_deliveries, event_user_delivery['total'], config.responses['page_size'], param_data, config.responses['thread_count'])
            event_user_delivery = event_user_delivery_collection['response']
        else:  # otherwise continue on with that initial request
            event_user_delivery = event_user_delivery['data']

        log.debug('Event ID: ' + event['eventId'] + ', retrieved event_user_delivery data: ' + json.dumps(event_user_delivery))
        log.info('Event ID: ' + event['eventId'] + ', retrieved event_user_delivery number: ' + str(len(event_user_delivery)))

        counter = 0

        for data in event_user_delivery:
            try:

                # temporary workaround implemented to resolve an issue where targetName isn't being provided
                if 'targetName' in data['person']:
                    user_name = data['person']['targetName']
                else:
                    log.debug('No targetName found for user id: ' + data['person']['id'] + ' attempting to get current targetName')
                    user_name = (xm_person.get_person(data['person']['id']))['targetName']  # by design to throw an exception if fails
                    log.debug('targetName received for ' + user_name)


                # find out how the user was initially targeted
                # the API does not currently identify if a user was targeted via a dynamic team
                # it does identify if the user was targeted via a group or if the user was targeted directly
                if user_name in peopleRecipients:
                    event_details["recipientTargetName"] = user_name
                    event_details['recipientTargetType'] = "PERSON"
                else:
                    event_details["recipientTargetName"] = dynamicTeamRecipients
                    event_details['recipientTargetType'] = "DYNAMIC TEAM"

                if "notifications" in data:
                    if "data" in data["notifications"]:
                        if data["notifications"]["count"] > 0:
                            for rec in data["notifications"]["data"]:
                                if rec["category"] == 'GROUP': #phew! we got here
                                    event_details["recipientTargetName"] = rec["recipient"]["targetName"]
                                    event_details['recipientTargetType'] = "GROUP"

                # build array of objects to write to CSV file
                if data['deliveryStatus'] == "RESPONDED":
                    csv_data.append(dict(targetName=user_name,
                                         response=data['response']['text'],
                                         event_created=str(event['created'].replace('+0000', "")),
                                         retrieved_date_time=str(current_date_time.isoformat()),
                                         delivery_status=data['deliveryStatus'],
                                         workflow=event_details['workflow_name'],
                                         form=event_details['form_name'],
                                         event_id=event_details['event_id'],
                                         # event_uuid=event_details['event_uuid'],
                                         recipientTargetName=str(event_details['recipientTargetName']),
                                         recipientTargetType=str(event_details['recipientTargetType'])))
                    counter = counter + 1
                elif data['deliveryStatus'] == "DELIVERED":
                    csv_data.append(dict(targetName=user_name,
                                         response="",
                                         event_created=str(event['created'].replace('+0000', "")),
                                         retrieved_date_time=str(current_date_time.isoformat()),
                                         delivery_status=data['deliveryStatus'],
                                         workflow=event_details['workflow_name'],
                                         form=event_details['form_name'],
                                         event_id=event_details['event_id'],
                                         # event_uuid=event_details['event_uuid'],
                                         recipientTargetName=str(event_details['recipientTargetName']),
                                         recipientTargetType=str(event_details['recipientTargetType'])))
                    counter = counter + 1
                else:
                    log.info('Not adding to csv writer array, unexpected information: ' + json.dumps(data))  # unlikely, but let's just log to make sure

            except Exception as e:
                log.error('Exception ' + str(e) + ' on line:  ' + str(data))

        log.info('Event ID ' + event['eventId'] + ' count of user delivery data added to csv writer array: ' + str(counter))

    log.info('Found Number of Rows for User Delivery Data: ' + str(len(csv_data)))

    if len(csv_data) > 0:
        try:
            with open(config.responses['file_name_new'], 'w', newline='', encoding=config.responses['encoding']) as csv_file_new:
                csv_writer = csv.writer(csv_file_new, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

                # write the header
                csv_writer.writerow(['key', 'targetName', 'response', 'event_created', 'retrieved_date_time', 'delivery_status', 'workflow', 'form', 'event_id', 'recipientTargetName', 'recipientTargetType'])

                # write the values
                for row in csv_data:
                    primary_key = row['targetName']+" "+ event_details['event_uuid']
                    csv_writer.writerow([primary_key, row['targetName'], row['response'], row['event_created'], row['retrieved_date_time'], row['delivery_status'], row['workflow'], row['form'], row['event_id'], row['recipientTargetName'], row['recipientTargetType']])

        except Exception as e:
            log.error('Exception while writing to csv file name: '+str(config.responses['file_name_new'])+' with exception: ' + str(e))

        try:
            with open(config.responses['file_name'], 'w', newline='',
                      encoding=config.responses['encoding']) as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

                # write the header
                csv_writer.writerow(
                    ['key', 'targetName', 'response', 'event_created', 'retrieved_date_time', 'delivery_status'])

                # write the values
                for row in csv_data:
                    primary_key = row['targetName'] + " " + row['event_created']
                    csv_writer.writerow([primary_key, row['targetName'], row['response'], row['event_created'],
                                         row['retrieved_date_time'], row['delivery_status']])

        except Exception as e:
            log.error('Exception while writing to csv file name: ' + str(
                config.responses['file_name']) + ' with exception: ' + str(e))


if __name__ == "__main__":
    # configure the logging
    logging.basicConfig(level=config.responses['logging']["level"], datefmt="%m-%d-%Y %H:%M:%Srm ",
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
                        handlers=[RotatingFileHandler(config.responses['logging']["file_name"], maxBytes=config.responses['logging']["max_bytes"],
                                                      backupCount=config.responses['logging']['back_up_count'])])
    log = logging.getLogger(__name__)

    # time start
    time_util = xmatters.TimeCalc()
    start = time_util.get_time_now()
    log.info("Starting Process: " + time_util.format_date_time_now(start))

    # instantiate classes
    environment = xmatters.xMattersAPI(config.environment["url"], config.environment["username"],
                                       config.environment["password"])
    xm_event = xmatters.xMattersEvent(environment)
    xm_collection = xmatters.xMattersCollection(environment)
    xm_person = xmatters.xMattersPerson(environment)

    main()  # execute the main process

    # end the duration
    end = time_util.get_time_now()
    log.info("Process Duration: " + time_util.get_diff(end, start))
