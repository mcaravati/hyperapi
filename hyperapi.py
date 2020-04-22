"""
    Downloads and scrapes a .ical file
"""
from datetime import datetime, timedelta
import re
from icalendar import Calendar
import pytz
import requests


class Lesson:
    """
        An event scrapped from the .ical file
    """

    def __init__(self, **kwargs):
        """
            Initializes the object variables
        :param kwargs: Constructor's arguments
        """

        self.lesson_id = self.name = \
            self.teacher = self.type = \
            self.room = ''

        self.lesson_id = kwargs.get('lesson_id')
        self.name = kwargs.get('name')
        self.teacher = kwargs.get('teacher')
        self.type = kwargs.get('type')
        self.room = kwargs.get('room')
        self.start_date = kwargs.get('start_date')
        self.end_date = kwargs.get('end_date')
        self.start_hour = kwargs.get('start_hour')
        self.start_db = kwargs.get('start_db')
        self.end_hour = kwargs.get('end_hour')
        self.end_db = kwargs.get('end_db')

    def show(self):
        """
            Displays a formatted text version of Lesson in the console, for human-readable trace

        :return:
            None
        """
        print("\n=== COURS ===\n" +
              "Type : {}\n" + self.lesson_id +
              "Nom du cours : {}\n" + self.name +
              "Professeur : {}\n" + self.teacher +
              "Type de cours : {}\n" + self.type +
              "Localisation : {}\n" + self.room +
              "Date de début : {}\n" + self.start_date +
              "Date de fin : {}\n" + self.end_date +
              "Heure de début {}\n" + self.start_hour +
              "Heure de fin : {}\n".format(self.lesson_id,
                                           self.name,
                                           self.teacher,
                                           self.type,
                                           "Salle inconnue" if self.room is None else self.room,
                                           self.start_date,
                                           self.end_date,
                                           self.start_hour,
                                           self.end_hour))

    def is_empty(self):
        """
            Checks if the event present in the .ical file is empty (may happen)

        :return:
            True if the event is empty
        """
        return self.name == '' and self.teacher == '' and (self.type == '' or self.type == 'Divers')

    def to_json(self):
        """
            Converts the Lesson object to a JSON array

        :return:
            A JSON array
        """
        return '{"dateDebut":"' + self.start_date + \
               '", "heureDebut":"' + self.start_hour + \
               '", "heureFin":"' + self.end_hour + \
               '", "idMatiere":"' + self.lesson_id + \
               '", "nomMatiere":"' + self.name + \
               '", "nomProf":"' + self.teacher + \
               '", "typeCours":"' + self.type + \
               '", "numeroSalle":"' + ("" if self.room is None else self.room) + \
               '", "listeDevoirs":""}'


def scrape(calendar: str):
    """
    Scrapes .ical file directly from URL

    :param calendar: The .ical file URL
    :return:
        A Lesson array
    """
    calendar = get_calendar(calendar)
    lesson_list = []

    for component in calendar.walk():
        if component.name == "VEVENT":
            lesson_list.append(event_filter(component))

    return lesson_list


def event_filter(event: dict):
    """
        Scrapes the .ical file and builds a Lesson object directly from it
    :param event: The text of the .ical file
    :return:
        A Lesson object fully initialized
    """
    event_type = event_id = event_name = event_teacher = event_room = event_start_date = \
        event_end_date = event_start_hour = event_end_hour = event_start_db = \
        event_end_db = ""

    header = event.get("SUMMARY").split(" - ")

    if len(header) == 3:
        if re.match("^M[0-9]*", header[0]):
            event_id = header[0].split(' ', 1)[0]
            event_name = header[0].split(' ', 1)[1]
        else:
            event_name = header[0]
        if re.match("M\.|Mme|_enseignant inconnu_", header[1]):
            event_teacher = header[1].strip()
        if re.match("TP|TD", header[2]):
            event_type = header[2]
        else:
            try:
                event_type = header[3]
            except IndexError:
                event_type = "None"
        try:
            event_room = event.get("LOCATION")
        except KeyError:
            pass

        event_start_date = event.get("DTSTART").dt.strftime("%Y-%m-%d")
        event_end_date = event.get("DTEND").dt.strftime("%Y-%m-%d")
        utc = pytz.UTC
        if event.get("DTEND").dt > utc.localize(datetime(2020, 3, 29)):
            event_start_hour = (
                event.get("DTSTART").dt +
                timedelta(hours=2)).strftime("%Hh%M")
            event_start_db = (
                event.get("DTSTART").dt +
                timedelta(hours=2)).strftime("%H:%M:%S")
            event_end_hour = (
                event.get("DTEND").dt +
                timedelta(hours=2)).strftime("%Hh%M")
            event_end_db = (
                event.get("DTEND").dt +
                timedelta(hours=2)).strftime("%H:%M:%S")
        else:
            event_start_hour = (
                event.get("DTSTART").dt +
                timedelta(hours=1)).strftime("%Hh%M")
            event_start_db = (
                event.get("DTSTART").dt +
                timedelta(hours=1)).strftime("%H:%M:%S")
            event_end_hour = (
                event.get("DTEND").dt +
                timedelta(hours=1)).strftime("%Hh%M")
            event_end_db = (
                event.get("DTEND").dt +
                timedelta(hours=1)).strftime("%H:%M:%S")

    return Lesson(lesson_id=event_id,
                  name=event_name,
                  teacher=event_teacher,
                  type=event_type,
                  room=event_room,
                  start_date=event_start_date,
                  end_date=event_end_date,
                  start_hour=event_start_hour,
                  start_db=event_start_db,
                  end_hour=event_end_hour,
                  end_db=event_end_db)


def get_calendar(url: str):
    """
        Downloads the .ical file from URL

    :param url: The .ical file URL
    :return:
        str
    """
    return Calendar.from_ical(requests.get(url).text)
