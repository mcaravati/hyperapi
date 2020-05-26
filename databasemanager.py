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
        self.last_session = None

        # DB tables creation
        connection = sqlite3.connect(self.database)
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'cours(idCours INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'idMatiere TEXT NOT NULL, nomMatiere TEXT NOT NULL, ' +
            'UNIQUE(idMatiere, nomMatiere));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'profs(idProf INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'nomProf TEXT NOT NULL, UNIQUE(nomProf));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'salles(idSalle INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'numeroSalle TEXT NOT NULL, UNIQUE(numeroSalle));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS ' +
            'classes(idClasse INTEGER PRIMARY KEY AUTOINCREMENT, ' +
            'nomClasse TEXT NOT NULL, UNIQUE(nomClasse));'
        )
        connection.execute(
            'CREATE TABLE IF NOT EXISTS sessions(' +
            'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
            'debut DATETIME NOT NULL,' +
            'fin DATETIME NOT NULL,' +
            'typeCours TEXT NOT NULL' +
            ');'
        )
        # Link profs-session
        connection.execute(
            'CREATE TABLE IF NOT EXISTS lienPSe(' +
            'idLienPSe INTEGER PRIMARY KEY AUTOINCREMENT,' +
            'idProf INTEGER NOT NULL,' +
            'idSession INTEGER NOT NULL,' +

            'FOREIGN KEY(idProf) REFERENCES profs(idProf),' +
            'FOREIGN KEY(idSession) REFERENCES sessions(idSession)' +
            ');'
        )
        # Link salles-sessions
        connection.execute(
            'CREATE TABLE IF NOT EXISTS lienSaSe(' +
            'idLienSaSe INTEGER PRIMARY KEY AUTOINCREMENT,' +
            'idSalle INTEGER NOT NULL,' +
            'idSession INTEGER NOT NULL,' +

            'FOREIGN KEY(idSalle) REFERENCES salles(idSalle),' +
            'FOREIGN KEY(idSession) REFERENCES sessions(idSession)' +
            ');'
        )
        # Link cours-sessions
        connection.execute(
            'CREATE TABLE IF NOT EXISTS lienCoSe(' +
            'idLienCoSe INTEGER PRIMARY KEY AUTOINCREMENT,' +
            'idCours INTEGER NOT NULL,' +
            'idSession INTEGER NOT NULL,' +

            'FOREIGN KEY(idCours) REFERENCES cours(idCours),' +
            'FOREIGN KEY(idSession) REFERENCES sessions(idSession)' +
            ');'
        )
        # Link classes-sessions
        connection.execute(
            'CREATE TABLE IF NOT EXISTS lienClSe(' +
            'idLienClSe INTEGER PRIMARY KEY AUTOINCREMENT,' +
            'idClasse INTEGER NOT NULL,' +
            'idSession INTEGER NOT NULL,' +

            'FOREIGN KEY(idClasse) REFERENCES classes(idClasse),' +
            'FOREIGN KEY(idSession) REFERENCES sessions(idSession)' +
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
        cursor = connection.cursor()

        # Delete all content to escape conflicts
        cursor.execute("DELETE FROM cours;")
        cursor.execute("DELETE FROM profs;")
        cursor.execute("DELETE FROM salles;")
        cursor.execute("DELETE FROM classes;")
        cursor.execute("DELETE FROM sessions;")
        cursor.execute("DELETE FROM lienSaSe;")
        cursor.execute("DELETE FROM lienPSe;")
        cursor.execute("DELETE FROM lienClSe;")
        cursor.execute("DELETE FROM lienCoSe;")

        connection.commit()

        for i in range(0, len(self.classes)):
            sessions_list = hyperapi.scrape(self.classes[i].url)
            school_class = self.classes[i].nom

            for session in sessions_list:
                if not session.is_empty():
                    try:
                        self.add_session(session, cursor)
                    except (sqlite3.IntegrityError, TypeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_room(session.numeroSalle, cursor)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_teacher(session.nomProf, cursor)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_course(session.idMatiere, session.nomMatiere, cursor)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)
                    try:
                        self.add_class(school_class, cursor)
                    except (sqlite3.IntegrityError, AttributeError) as exception:
                        LOGGER.exception("%s occured while adding a session",
                                         type(exception).__name__)

        connection.commit()
        connection.close()
        LOGGER.debug(
            "[+] Database has been updated : {}"
            .format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

    def add_room(self, room_list: str, cursor: sqlite3.Cursor):
        """
            Adds a numeroSalle list to the database
        :param room_list: The desired numeroSalle list
        :param cursor: The SQL cursor
        :return:
            None
        """
        rooms = room_list.split(",")
        for room in rooms:
            cursor.execute(
                'INSERT OR IGNORE INTO salles(numeroSalle) VALUES("' + room + '");'
            )
            id = cursor.execute('SELECT idSalle FROM salles WHERE numeroSalle="' + room + '"').fetchone()[0]
            cursor.execute(
                'INSERT OR IGNORE INTO lienSaSe(idSession, idSalle) VALUES(' +
                self.last_session +
                ', ' +
                str(id) +
                ');'
            )

    def add_teacher(self, teachers_list: str, cursor:sqlite3.Cursor):
        """
            Adds a list of teachers to the database
        :param teachers_list: A list of teachers
        :param cursor: The SQL cursor
        :return:
            None
        """
        teachers = teachers_list.split(", ")
        for teacher in teachers:
            cursor.execute('INSERT OR IGNORE INTO profs(nomProf) VALUES("' + teacher + '");')
            id = cursor.execute('SELECT idProf FROM profs WHERE nomProf="' + teacher + '"').fetchone()[0]
            cursor.execute(
                'INSERT OR IGNORE INTO lienPSe(idSession, idProf) VALUES(' +
                self.last_session +
                ', ' +
                str(id) +
                ');'
            )

    def add_course(self, course_id: str, course_name: str, cursor:sqlite3.Cursor):
        """
            Adds a course to the database
        :param course_id: The course ID retrieved from Hyperplanning
        :param course_name: The course nomMatiere
        :param cursor: The SQL cursor
        :return:
            None
        """
        cursor.execute(
            'INSERT OR IGNORE INTO cours(idMatiere, nomMatiere) VALUES("'
            + course_id
            + '", "'
            + course_name
            + '");'
        )
        id = cursor.execute('SELECT idCours FROM cours ' +
                       'WHERE idMatiere="' + course_id + '" ' +
                       'AND nomMatiere="' + course_name + '"').fetchone()[0]
        cursor.execute(
            'INSERT OR IGNORE INTO lienCoSe(idSession, idCours) VALUES(' +
            self.last_session +
            ', ' +
            str(id) +
            ');'
        )

    def add_class(self, school_class: str, cursor:sqlite3.Cursor):
        """
            Adds a school class to the database
        :param school_class: The desired school class
        :param cursor: The SQL cursor
        :return:
            None
        """
        cursor.execute('INSERT OR IGNORE INTO classes(nomClasse) VALUES("' +
                           school_class + '");')
        id = cursor.execute('SELECT idClasse FROM classes WHERE nomClasse="' + school_class + '"').fetchone()[0]
        cursor.execute(
            'INSERT OR IGNORE INTO lienClSe(idSession, idClasse) VALUES(' +
            self.last_session +
            ', ' +
            str(id) +
            ');'
        )

    def add_session(self, session: hyperapi.Lesson, cursor:sqlite3.Cursor):
        """
            Adds a session to the database
        :param session: The desired session
        :param cursor: The SQL cursor
        :return:
            None
        """
        cursor.execute(
            """INSERT OR IGNORE INTO sessions(debut, fin, """ +
            """typeCours) VALUES (\'"""
            + session.dateDebut
            + """ """
            + session.start_db
            + """\', \'"""
            + session.dateFin
            + """ """
            + session.end_db
            + """\', \'"""
            + session.typeCours
            + """\');"""
        )
        self.last_session = str(cursor.lastrowid)

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

        sessions_results = cursor.execute(
            'SELECT sessions.debut,' +
            'sessions.fin,' +
            'cours.idMatiere,' +
            'cours.nomMatiere,' +
            'sessions.id,' +
            'salles.numeroSalle,' +
            'sessions.typeCours ' +
            'FROM sessions ' +

            'INNER JOIN lienClSe ON lienClSe.idSession = sessions.id ' +
            'INNER JOIN classes ON classes.idClasse = lienClSe.idClasse ' +

            'INNER JOIN lienSaSe ON lienSaSe.idSession = sessions.id ' +
            'INNER JOIN salles ON salles.idSalle = lienSaSe.idSalle ' +

            'INNER JOIN lienCoSe ON lienCoSe.idSession = sessions.id ' +
            'INNER JOIN cours ON cours.idCours = lienCoSe.idCours ' +

            'WHERE classes.nomClasse = "' +
            school_class +
            '" AND sessions.debut BETWEEN "' +
            begin +
            '" AND "' +
            end +
            '" ORDER BY debut;'
        ).fetchall()

        for session in sessions_results:
            profs = cursor.execute('SELECT profs.nomProf FROM sessions ' +
                                   'INNER JOIN lienPSe ON lienPSe.idSession = sessions.id ' +
                                   'INNER JOIN profs ON lienPSe.idProf = profs.idProf ' +
                                   'WHERE sessions.id = ' + str(session[4])).fetchall()
            new_str = ""
            for o in profs:
                new_str += o[0] + ", "
            new_str = new_str[:-2]
            try:
                sessions_list.append(
                    hyperapi.Lesson(
                        idMatiere=session[2],
                        nomMatiere=session[3],
                        nomProf=new_str,
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
