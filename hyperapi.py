"""
    Downloads and scrapes a .ical file
"""
from datetime import datetime, timedelta
import re
from icalendar import Calendar
import pytz
import requests

REGEX_ID = re.compile("^M[0-9]+")
REGEX_TEACHERS = re.compile("M\.|Mme|_enseignant inconnu_")
REGEX_TYPE = re.compile("TP_*|TD_*|Cours_*")
REGEX_DS = re.compile("DS_*")


class Lesson:
    """
        An event scrapped from the .ical file
    """

    def __init__(self, **kwargs):
        """
            Initializes the object variables
        :param kwargs: Constructor's arguments
        """

        self.idMatiere = kwargs.get('idMatiere') or "ID inconnu"
        self.nomMatiere = kwargs.get('nomMatiere') or "Nom inconnu"
        self.nomProf = kwargs.get('nomProf') or "Enseignant inconnu"
        self.typeCours = kwargs.get('typeCours') or "Type inconnu"
        self.numeroSalle = kwargs.get('numeroSalle') or "Salle inconnue"
        self.dateDebut = kwargs.get('dateDebut') or "Date inconnue"
        self.dateFin = kwargs.get('dateFin') or "Date inconnue"
        self.heureDebut = kwargs.get('heureDebut') or "Heure inconnue"
        self.start_db = kwargs.get('start_db') or ""
        self.heureFin = kwargs.get('heureFin') or "Heure inconnue"
        self.end_db = kwargs.get('end_db') or ""
        self.listeDevoirs = ""

    def is_empty(self):
        """
            Checks if the event present in the .ical file is empty (may happen)

        :return:
            True if the event is empty
        """
        return self.nomMatiere == '' and self.nomProf == '' and (self.typeCours == '' or self.typeCours == 'Divers')


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

    regex_result = list(filter(REGEX_ID.match, header))
    if regex_result:
        event_id = regex_result[0].split(' ')[0]
        event_name = regex_result[0].split(' ')[1]
    else:
        event_name = header[0]

    regex_result = list(filter(REGEX_TEACHERS.match, header))
    if regex_result:
        event_teacher = regex_result[0].strip()

    regex_result = list(filter(REGEX_TYPE.match, header))
    if regex_result:
        event_type = regex_result[0]

    regex_result = list(filter(REGEX_DS.match, header))
    if regex_result:
        event_type = regex_result[0]
        event_name += " : " + header[len(header) - 2]

    try:
        event_room = event.get("LOCATION")
    except KeyError:
        pass

    event_start_date = event.get("DTSTART").dt.strftime("%Y-%m-%d")
    event_end_date = event.get("DTEND").dt.strftime("%Y-%m-%d")
    if len(str(event.get("DTSTART").dt).split(' ')) != 1:
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

    return Lesson(idMatiere=event_id,
                  nomMatiere=event_name,
                  nomProf=event_teacher,
                  typeCours=event_type,
                  numeroSalle=event_room,
                  dateDebut=event_start_date,
                  dateFin=event_end_date,
                  heureDebut=event_start_hour,
                  start_db=event_start_db,
                  heureFin=event_end_hour,
                  end_db=event_end_db)


def get_calendar(url: str):
    """
        Downloads the .ical file from URL

    :param url: The .ical file URL
    :return:
        str
    """
    return Calendar.from_ical(requests.get(url).text)
