"""
    Handles the .ical to database conversion.
"""
import sqlite3
import threading
import datetime
from collections import namedtuple
import logging
import isoweek
import hyperapi
import json

LOGGER = logging.getLogger('root')
LOGGER.addHandler(logging.FileHandler("logs/database.log", 'w'))
Classe = namedtuple("Classe", ("nom", "url"), defaults=(None,))


def create_class(name: str, url: str):
    """
        Creates an initialized Classe namedtuple
    :param name: The nomMatiere of the school class
    :param url: The URL of the appropriate .ical file
    :return:
        An initialized Classe namedtuple object
    """
    return Classe(name, "https://hyperplanning.iut.u-bordeaux.fr/" +
                  "Telechargements/ical/Edt_EXAMPLE.ics?" +
                  "version=2019.0.5.0&idICal={}&param=643d5b312e2e36325d2666683d3126663d31".format(
                      url))

def parse_config():
    """
        Parses the calendar config file
    :return: A Classe list
    """
    school_class_list = []
    with open('config/calendars.config', 'r') as config:
        for line in config.readlines():
            school_class = line.split(':')
            school_class_list.append(create_class(school_class[0],
                                                  school_class[1].replace('\n', '')))
    return school_class_list

class DatabaseManager:
    """
        Class to handle the .ical to database conversion
    """
    def __init__(self, database):
        """
            Saves the school classes and builds the database
        :param database: The desired database
        """
        self.database = database
        self.classes = parse_config()

        # DB tables creation
        connection = sqlite3.connect(self.database)
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'cours(id INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'idMatiere TEXT NOT NULL, nomMatiere TEXT NOT NULL, ' +
            'UNIQUE(idMatiere, nomMatiere));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'profs(id INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'nomProf TEXT NOT NULL, UNIQUE(nomProf));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'salles(id INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'numeroSalle TEXT NOT NULL, UNIQUE(numeroSalle));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'classes(id INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'nomClasse TEXT NOT NULL, UNIQUE(nomClasse));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'groupes(id INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'nomClasse TEXT NOT NULL, nomGroupe TEXT NOT NULL, UNIQUE(nomGroupe));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS sessions(' +
            'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
            'debut DATETIME NOT NULL,' +
            'fin DATETIME NOT NULL,' +
            'idMatiere INTEGER NOT NULL,' +
            'nomMatiere INTEGER NOT NULL,' +
            'nomProf INTEGER NOT NULL,' +
            'numeroSalle INTEGER NOT NULL,' +
            'typeCours TEXT NOT NULL,' +
            'nomClasse INTEGER NOT NULL,' +

            'FOREIGN KEY(idMatiere) REFERENCES cours(id),' +
            'FOREIGN KEY(nomMatiere) REFERENCES cours(id),' +
            'FOREIGN KEY(nomProf) REFERENCES profs(id),' +
            'FOREIGN KEY(numeroSalle) REFERENCES salles(id),' +
            'FOREIGN KEY(nomClasse) REFERENCES classes(id)' +
            ');'
        )
        connection.close()

    def build(self):
        """
            Updates the database every hour
        :return:
            None
        """
        # Recursive function to update the DB each hour
        threading.Timer(3600.00, self.build).start()

        connection = sqlite3.connect(self.database)

        # Delete all content to escape conflicts
        connection.execute("DELETE FROM cours;")
        connection.execute("DELETE FROM profs;")
        connection.execute("DELETE FROM salles;")
        connection.execute("DELETE FROM classes;")
        connection.execute("DELETE FROM groupes;")
        connection.execute("DELETE FROM sessions;")
        connection.commit()

        for i in range(0, len(self.classes)):
            sessions_list = hyperapi.scrape(self.classes[i].url)
            school_class = self.classes[i].nom

            for session in sessions_list:
                if not session.is_empty():
                    try:
                        self.add_room(session.numeroSalle, connection)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_teacher(session.nomProf, connection)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_course(session.idMatiere, session.nomMatiere, connection)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_class(school_class, connection)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_session(session, school_class, connection)
                    except (sqlite3.IntegrityError, TypeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)

        connection.commit()
        connection.close()
        LOGGER.debug(
            "[+] Database has been updated : {}"
            .format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

    @staticmethod
    def add_room(room_list: str, connection):
        """
            Adds a numeroSalle list to the database
        :param room_list: The desired numeroSalle list
        :param connection: The SQL connection
        :return:
            None
        """
        rooms = room_list.split(",")
        for room in rooms:
            connection.execute(
                'INSERT OR IGNORE INTO salles(numeroSalle) VALUES("' + room + '");'
            )

    @staticmethod
    def add_teacher(teachers_list: str, connection):
        """
            Adds a list of teachers to the database
        :param teachers_list: A list of teachers
        :param connection: The SQL connection
        :return:
            None
        """
        teachers = teachers_list.split(", ")
        for teacher in teachers:
            connection.execute('INSERT OR IGNORE INTO profs(nomProf) VALUES("' + teacher + '");')

    @staticmethod
    def add_course(course_id: str, course_name: str, connection):
        """
            Adds a course to the database
        :param course_id: The course ID retrieved from Hyperplanning
        :param course_name: The course nomMatiere
        :param connection: The SQL connection
        :return:
            None
        """
        connection.execute(
            'INSERT OR IGNORE INTO cours(idMatiere, nomMatiere) VALUES("'
            + course_id
            + '", "'
            + course_name
            + '");'
        )

    @staticmethod
    def add_class(school_class: str, connection):
        """
            Adds a school class to the database
        :param school_class: The desired school class
        :param connection: The SQL connection
        :return:
            None
        """
        connection.execute('INSERT OR IGNORE INTO classes(nomClasse) VALUES("' +
                           school_class + '");')

    @staticmethod
    def add_session(session: hyperapi.Lesson, school_class: str, connection):
        """
            Adds a session to the database
        :param session: The desired session
        :param school_class: The concerned school class
        :param connection: The SQL connection
        :return:
            None
        """
        connection.execute(
            """INSERT OR IGNORE INTO sessions(debut, fin, idMatiere, nomMatiere, """ +
            """nomProf, numeroSalle, typeCours, nomClasse) VALUES (\'"""
            + session.dateDebut
            + """ """
            + session.start_db
            + """\', \'"""
            + session.dateFin
            + """ """
            + session.end_db
            + '''\', (SELECT id FROM cours WHERE idMatiere=\"'''
            + session.idMatiere
            + '''\" AND nomMatiere=\"'''
            + session.nomMatiere
            + '''\"), (SELECT id FROM cours WHERE nomMatiere=\"'''
            + session.nomMatiere
            + '''\"), (SELECT id FROM profs WHERE nomProf=\"'''
            + session.nomProf
            + '''\"),(SELECT id FROM salles WHERE numeroSalle=\"'''
            + session.numeroSalle
            + '''\"), \"'''
            + session.typeCours
            + '''\",(SELECT id FROM classes WHERE nomClasse=\"'''
            + school_class
            + """\"));"""
        )

    def get_sql(self, school_class: str, **kwargs):
        """
            Retrieves the desired sessions from the database
        :param school_class: The desired school class
        :param kwargs: Desired bounds
        :return:
            A JSON array or None
        """
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        sessions_list = []

        if "week" in kwargs:
            iso_date = kwargs.get("week")
            week = isoweek.Week(
                int(iso_date.split("-W")[0]), int(iso_date.split("-W")[1])
            )
            begin = week.monday().strftime("%Y-%m-%d")
            end = week.sunday().strftime("%Y-%m-%d")
        else:
            day = kwargs.get("day")
            begin = str(day.strftime("%Y-%m-%d"))
            end = str((day + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))

        results = cursor.execute(
            'SELECT DISTINCT debut, fin, cours.idMatiere, ' +
            'cours.nomMatiere, profs.nomProf, salles.numeroSalle, typeCours ' +
            'FROM sessions INNER JOIN cours ON sessions.idMatiere=cours.id ' +
            'INNER JOIN profs ON sessions.nomProf=profs.id ' +
            'INNER JOIN salles ON sessions.numeroSalle=salles.id ' +
            'WHERE nomClasse=(SELECT id FROM classes WHERE nomClasse="'
            + school_class
            + '") AND debut BETWEEN "'
            + begin
            + '" AND "'
            + end
            + '" ORDER BY debut;'
        ).fetchall()

        for session in results:
            try:
                sessions_list.append(
                    hyperapi.Lesson(
                        idMatiere=session[2],
                        nomMatiere=session[3],
                        nomProf=session[4],
                        typeCours=session[6],
                        numeroSalle=session[5],
                        dateDebut=session[0].split()[0],
                        dateFin=session[1].split()[0],
                        heureDebut=datetime.datetime.strptime(
                            session[0].split()[1], "%H:%M:%S"
                            ).strftime("%Hh%M"),
                        heureFin=datetime.datetime.strptime(
                            session[1].split()[1], "%H:%M:%S"
                            ).strftime("%Hh%M"),
                    )
                )
            except IndexError as e:
                LOGGER.exception("%s : day is work-free")
                pass

        # JSON building
        return json.dumps(sessions_list, default=lambda o: o.__dict__)
