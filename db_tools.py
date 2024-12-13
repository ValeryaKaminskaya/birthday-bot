"""
This module provides a collection of database tools
for working with database for telegram bot with predictions.
"""
from datetime import datetime

import pyodbc
from typing import List, Tuple, Union
from enum import Enum
import logging
from contextlib import closing
import time

import secrets
from utils import setup_logger


class ApprovalStates(Enum):
    APPROVED = 'approved'
    NOT_APPROVED = 'not approved'
    REJECTED = 'rejected'
    INAPPROPRIATE = 'inappropriate'

class UserStates(Enum):
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    BANNED = 'banned'


class DBTools:
    # TODO: add docstring

    retries = 5
    delay = 1

    USERS_TABLE_NAME = 'birthday_bot_users'
    CREATE_USERS_TABLE_QUERY: str = f'''
        CREATE TABLE {USERS_TABLE_NAME} (
            user_id BIGINT NOT NULL PRIMARY KEY,
            user_name NVARCHAR(1024) NOT NULL,
            state NVARCHAR(64) NOT NULL DEFAULT '{UserStates.ACTIVE.value}'
        )
    '''
    BIRTHDAYS_TABLE_NAME = 'birthday_bot_birthdays'
    CREATE_BIRTHDAYS_TABLE_QUERY: str = f'''
        CREATE TABLE {BIRTHDAYS_TABLE_NAME} (
            user_id BIGINT,
            chat_id BIGINT,
            day TINYINT,
            month TINYINT,
            PRIMARY KEY(user_id, chat_id),
            FOREIGN KEY(user_id) REFERENCES {USERS_TABLE_NAME}(user_id)
        )
    '''
    # for which chat user registered they birthday (what chat need to be notified)
    USER_CHATS_TABLE_NAME = 'birthday_bot_user_chats'
    CREATE_USER_CHATS_TABLE_QUERY = f'''
        CREATE TABLE {USER_CHATS_TABLE_NAME} (
            user_id BIGINT,
            chat_id BIGINT,
            PRIMARY KEY(user_id, chat_id),
            FOREIGN KEY(user_id) REFERENCES {USERS_TABLE_NAME}(user_id)
        )
    '''
    # custom congratulations, user+chat specific
    CONGRATULATIONS_TABLE_NAME = 'birthday_bot_congratulations'
    CONGRATULATIONS_TABLE_QUERY = f'''
        CREATE TABLE {CONGRATULATIONS_TABLE_NAME} (
            user_id BIGINT,
            chat_id BIGINT,
            congratulation NVARCHAR(1024),
            PRIMARY KEY(user_id, chat_id),
            FOREIGN KEY(user_id) REFERENCES {USERS_TABLE_NAME}(user_id)
        )
    '''

    CHECK_IF_TABLE_EXISTS_QUERY = (
        'SELECT name FROM sys.tables '
        # TODO: check
        'WHERE name= ?'
    )

    ADD_USER_QUERY = (
        f'INSERT INTO {USERS_TABLE_NAME}\n'
        '(user_id, user_name, state)\n'
        f'VALUES (?, ?, \'{UserStates.ACTIVE.value}\')'
    )
    ADD_USER_CHAT_QUERY = (
        f'INSERT INTO {USER_CHATS_TABLE_NAME}\n'
        '(user_id, chat_id)\n'
        f'VALUES (?, ?)'
    )
    ADD_CONGRATULATION_QUERY = (
        f'INSERT INTO {CONGRATULATIONS_TABLE_NAME}\n'
        '(user_id, chat_id, congratulation)\n'
        f'VALUES (?, ?, ?)'
    )
    CHECK_USER_EXISTS_QUERY: str = (
        f'SELECT user_id FROM {USERS_TABLE_NAME} WHERE user_id = ?'
    )
    CHECK_USER_REGISTERED_IN_CHAT_QUERY = (
        f'SELECT user_id FROM {USER_CHATS_TABLE_NAME} WHERE user_id = ? '
        f'AND chat_id = ?'
    )
    BAN_USER_QUERY = (
        f'UPDATE {USERS_TABLE_NAME} SET state = \'{UserStates.BANNED.value}\' '
        'WHERE user_id = ?'
    )
    CHECK_USER_ALREADY_REGISTERED_BIRTHDAY_QUERY = (
        f'SELECT day, month FROM {BIRTHDAYS_TABLE_NAME} WHERE user_id = ? '
        f'and chat_id = ?'
    )
    UPDATE_USER_BIRTHDAY_QUERY = (
        f'UPDATE {BIRTHDAYS_TABLE_NAME} SET day = ?, month = ? '
        f'WHERE chat_id =? and user_id = ?'
    )
    CHECK_ALREADY_REGISTERED_CONGRATULATIONS_QUERY = (
        f'SELECT congratulation FROM {CONGRATULATIONS_TABLE_NAME} '
        f'WHERE user_id = ? and chat_id = ?'
    )
    UPDATE_CONGRATULATIONS_QUERY = (
        f'UPDATE {CONGRATULATIONS_TABLE_NAME} SET congratulation = ? '
        f'WHERE user_id = ? and chat_id = ?'
    )
    ADD_USER_BIRTHDAY_QUERY = (
        f'INSERT INTO {BIRTHDAYS_TABLE_NAME}\n'
        '(user_id, chat_id, day, month)\n'
        f'VALUES (?, ?, ?, ?)'
    )
    DELETE_USER_BIRTHDAY_QUERY = (
        f'DELETE FROM {BIRTHDAYS_TABLE_NAME}\n'
        'WHERE user_id = ?\n'
        'AND chat_id = ?'
    )
    GET_USERS_WITH_BIRTHDAY_QUERY = (
        'SELECT birthdays.user_id, birthdays.chat_id, users.user_name\n'
        f'FROM {USERS_TABLE_NAME} users\n'
        f'LEFT JOIN {BIRTHDAYS_TABLE_NAME} birthdays\n'
        f'ON users.user_id = birthdays.user_id\n'
        f'WHERE users.state != \'{UserStates.BANNED.value}\'\n'
        'and birthdays.day = ? AND birthdays.month = ? '
        'and birthdays.chat_id = ?'
    )
    GET_USER_CONGRATULATION_QUERY = (
        f'SELECT congratulation FROM {CONGRATULATIONS_TABLE_NAME} '
        f'WHERE user_id = ? and chat_id = ?'
    )

    def __init__(
        self, logging_level: int = logging.INFO
    ):
        self.logging_level = logging_level
        setup_logger(self.__class__.__name__, level=logging_level)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug('Initial creating connection to DB')
        self._connection = self._connect_with_retry()
        if (
            not self.check_if_table_exists(self.USERS_TABLE_NAME)
            and not self.check_if_table_exists(self.BIRTHDAYS_TABLE_NAME)
            and not self.check_if_table_exists(self.USER_CHATS_TABLE_NAME)
            and not self.check_if_table_exists(self.CONGRATULATIONS_TABLE_NAME)
        ):
            self.initialize_tables()

    def _connect_with_retry(self) -> pyodbc.Connection:
        """
        Connect to the database with retry logic.

        :return: pyodbc.Connection object or raises an error
        """
        attempt = 0
        while attempt < self.retries:
            self.logger.debug('Connecting to DB, retry %s', attempt + 1)
            try:
                return pyodbc.connect(secrets.DB_CONN_STRING)
            except pyodbc.OperationalError as ex:
                attempt += 1
                self.logger.warning(
                    'Attempt %s/%s: Could not connect to the database. '
                    'Retrying in %s seconds...',
                    attempt, self.retries, self.delay
                )
                time.sleep(self.delay)
                if attempt == self.retries:
                    raise ex

    def get_connection(self) -> pyodbc.Connection:
        if self._connection is None or self._connection.closed:
            self.logger.debug('(Re)create connection')
            self._connection = self._connect_with_retry()

        return self._connection

    def initialize_tables(self) -> None:
        """
        Method to initialize tables by executing SQL queries.

        :return: None
        """
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                self.logger.debug(self.CREATE_USERS_TABLE_QUERY)
                cursor.execute(self.CREATE_USERS_TABLE_QUERY)
                self.logger.debug(self.CREATE_BIRTHDAYS_TABLE_QUERY)
                cursor.execute(self.CREATE_BIRTHDAYS_TABLE_QUERY)
                self.logger.debug(self.CREATE_USER_CHATS_TABLE_QUERY)
                cursor.execute(self.CREATE_USER_CHATS_TABLE_QUERY)
                self.logger.debug(self.CONGRATULATIONS_TABLE_QUERY)
                cursor.execute(self.CONGRATULATIONS_TABLE_QUERY)

    def check_if_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        :param table_name: The name of the table to check.
        :type table_name: str
        :return: True if the table exists, False otherwise.
        :rtype: bool
        """
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(self.CHECK_IF_TABLE_EXISTS_QUERY, (table_name,))
                data = cursor.fetchall()

        return len(data) > 0

    def execute_query(self, query: str, parameters: Tuple = ()) -> None:
        """
        Execute a database query with optional parameters.

        :param query: The SQL query to be executed.
        :type query: str
        :param parameters: The parameters to be substituted in the query.
            Default is an empty tuple.
        :type parameters: Tuple
        :return: The result cursor after executing the query.
        :rtype: None
        """
        self.logger.debug(
            'Running query:\n%s\nwith parameters: %s',
            query, parameters
        )
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, parameters)

    def fetch_one(
        self, query: str, parameters: Tuple = ()
    ) -> Union[Tuple, None]:
        """
        Fetch a single row from the database based on the given query
        and parameters.

        :param query: The SQL query to be executed.
        :param parameters: The parameters to be used in the query
            (default empty tuple).
        :return: A tuple representing the fetched row,
            or None if no row was found.
        """
        self.logger.debug(
            'Fetching one row with query:\n%s\nwith parameters: %s',
            query, parameters
        )
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, parameters)
                result = cursor.fetchone()

        return result

    def fetch_all(self, query: str, parameters: Tuple = ()) -> List[Tuple]:
        """
        Fetches all the rows returned by the given SQL query
        with optional parameters.

        :param query: The SQL query to execute.
        :param parameters: The parameters to be passed with the query
            (default is an empty tuple).
        :return: A list of tuples containing the fetched rows.
        """
        self.logger.debug(
            'Fetching all rows with query:\n%s\nwith parameters: %s',
            query, parameters
        )
        with self.get_connection() as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(query, parameters)
                result = cursor.fetchall()

        return result

    def user_exists(self, user_id: int) -> bool:
        """
        Checks if a user with the given ID exists in the database.

        Args:
            user_id (int): The ID of the user.

        Returns:
            bool: True if the user exists, False otherwise.
        """
        user = self.fetch_one(
            self.CHECK_USER_EXISTS_QUERY, (user_id, )
        )

        return user is not None

    def user_registered_in_chat(self, user_id: int, chat_id: int) -> bool:
        """
        Checks if a user is registered in a specific chat.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): Telegram chat ID.
    
        Returns:
            bool: True if the user is registered in the chat, False otherwise.
        """
        user = self.fetch_one(
            self.CHECK_USER_REGISTERED_IN_CHAT_QUERY, (user_id, chat_id)
        )

        return user is not None

    def add_user(self, user_id: int, user_name: str) -> None:
        """
        Adds a user to the users table.

        Args:
            user_id (int): The ID of the user.
            user_name (str): Telegram username of the user.
        """
        self.logger.debug('Adding user')
        self.execute_query(self.ADD_USER_QUERY, (user_id, user_name))

    def add_user_chat(self, user_id: int, chat_id: int) -> None:
        """
        Adds a user to the users table.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): Telegram chat id in which user registered
                            their birthday.
        """
        self.execute_query(
            self.ADD_USER_CHAT_QUERY, (user_id, chat_id)
        )

    def check_if_user_already_registered_birthday(self, user_id: int, chat_id: int) -> [None, str]:
        """
        Checks if a user has already registered a birthday in the database.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.

        Returns:
            bool: True if the user has already registered a birthday, False otherwise.
        """
        self.logger.debug('Checking if user already registered birthday')
        result = self.fetch_one(
            self.CHECK_USER_ALREADY_REGISTERED_BIRTHDAY_QUERY, (user_id, chat_id)
        )
        return result is not None

    def add_user_birthday(
        self, user_id: int, chat_id: int, day: int, month: int
    ) -> None:
        """
        Adds a user to the users table.

        Args:
            user_id (int): The ID of the user.
            chat_id: Telegram chat id.
            day: day of birth
            month: month of birth
        """
        if not self.check_if_user_already_registered_birthday(user_id, chat_id):
            self.logger.debug('User does not have birthday, adding it')
            self.logger.debug('User have no birthday, adding it')
            self.execute_query(
                self.ADD_USER_BIRTHDAY_QUERY,
                (user_id, chat_id, day, month)
            )
        else:
            self.logger.debug('User already had birthday, updating it')
            self.execute_query(
                self.UPDATE_USER_BIRTHDAY_QUERY,
                (day, month, chat_id, user_id)
            )

        return

    def ban_user(self, user_id: int) -> None:
        """
        Bans a user by setting their status to "banned".
    
        :param user_id: The ID of the user to ban.
        :return: None
        """
        self.logger.debug('Banning user: %s', user_id)
        self.execute_query(
            self.BAN_USER_QUERY, (user_id, )
        )

    def check_if_user_already_have_congratulations(
        self, user_id: int, chat_id: int
    ) -> [None, str]:
        """
        Checks if a user has already registered a birthday in the database.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): Telegram chat id in which user using the bot.

        Returns:
            bool: True if the user has already registered a birthday, False otherwise.
        """
        self.logger.debug('Checking if user already registered congratulations')
        result = self.fetch_one(
            self.CHECK_ALREADY_REGISTERED_CONGRATULATIONS_QUERY, (user_id, chat_id)
        )
        return result is not None

    def add_congratulation(self, user_id: int, chat_id: int, congratulation: str) -> None:
        """
        Adds or updates a congratulation message for a user in a specific chat.
    
        Args:
            user_id (int): The ID of the user.
            chat_id (int): Telegram chat ID.
            congratulation (str): The congratulation message to be stored.
    
        Raises:
            ValueError: If `user_id` or `chat_id` is not an integer.
        """
        if not isinstance(user_id, int):
            raise ValueError("user_id must be an integer.")
        if not isinstance(chat_id, int):
            raise ValueError("chat_id must be an integer.")
    
        self.logger.debug('Adding congratulation')
        self.logger.debug(
            f'user_id: {user_id}, chat_id: {chat_id}, congratulation: {congratulation}'
        )
        if not self.check_if_user_already_have_congratulations(user_id, chat_id):
            self.logger.debug('User does not have congratulation, adding it')
            self.execute_query(
                self.ADD_CONGRATULATION_QUERY, (user_id, chat_id, congratulation)
            )
        else:
            self.logger.debug('User already had congratulation, updating it')
            self.execute_query(
                self.UPDATE_CONGRATULATIONS_QUERY, (congratulation, user_id, chat_id)
            )

    def delete_user_birthday(self, user_id: int, chat_id: int) -> None:
        """
        Deletes the birthday record for a user in a specific chat.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): Telegram chat id.
        """
        # TODO: ?delete from birthday_bot_congratulations too?
        # TODO: ?delete from birthday_bot_user_chats too?
        # TODO: delete from all tables? (birthday_bot_birthdays,
        #  birthday_bot_user_chats, birthday_bot_congratulations, birthday_bot_users)
        self.logger.debug(f'Deleting birthday for user_id: {user_id} in chat_id: {chat_id}')
        self.execute_query(
            self.DELETE_USER_BIRTHDAY_QUERY, (user_id, chat_id)
        )
        
    def get_users_with_birthday(
        self, day: int, month: int, chat_id: int
    ) -> Union[List[Tuple], None]:
        """
        Retrieves users who have a birthday on the specified day and month.

        Args:
            day (int): The day of the birthday.
            month (int): The month of the birthday.
            chat_id (int): id of chat.

        Returns:
            List[Tuple]: A list of tuples, where each tuple contains
            the user_id, chat_id, and other relevant information of users
            with a birthday on the specified day and month.
        """
        self.logger.debug(f'Fetching users with birthdays on day: {day}, month: {month}')
        result = self.fetch_all(
            self.GET_USERS_WITH_BIRTHDAY_QUERY, (day, month, chat_id)
        )

        return result

    def get_user_congratulation(self, user_id: int, chat_id: int) -> [str, None]:
        """
        Retrieves the congratulation message for a user in a specific chat.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): Telegram chat id in which the user is using the bot.

        Returns:
            Union[str, None]: The congratulation message if it exists, None otherwise.
        """
        self.logger.debug(f'Fetching congratulation for user_id: {user_id} in chat_id: {chat_id}')
        result = self.fetch_one(self.GET_USER_CONGRATULATION_QUERY, (user_id, chat_id))

        return result[0] if result else None


if __name__ == '__main__':
    db_tools = DBTools(logging_level=logging.DEBUG)
