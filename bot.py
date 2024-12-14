# TODO: add docstring
import asyncio
import logging
import argparse
from datetime import datetime, date

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import constants
import secrets
from db_tools import DBTools
from utils import setup_logger


# noinspection SpellCheckingInspection
class BirthdayBot:
    # TODO: add docstring
    # TODO: catch exceptions in every command that uses user input
    # TODO: check how it works when you use commands in bot's private messages
    # noinspection NonAsciiCharacters
    default_congratulation = 'С днем рождения! 🎉'

    def __init__(
        self,
        token: str = secrets.API_TOKEN,
        test_run: bool = False,
        logging_level: int = logging.INFO
    ):
        self.token = token
        self.db_tools = DBTools(logging_level)
        self.logging_level = logging_level
        self.test_run = test_run
        self.log_file = (
            f'logs/{self.__class__.__name__}.log'
            if not test_run
            else f'logs/{self.__class__.__name__}_dev.log'
        )
        setup_logger(
            self.__class__.__name__,
            log_file=self.log_file,
            level=logging_level
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        if self.test_run:
            self.logger.warning('It is a test run')
        self.logger.debug(
            'Initialised %s', self.logger.name
        )

        # Initialize Bot instance with a default parse mode which will be passed to all API calls
        self.bot = Bot(self.token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

        # All handlers should be attached to the Router (or Dispatcher)
        self.dp = Dispatcher(storage=MemoryStorage())
        # Router instance creation
        self.router = Router()
        self.dp.include_router(self.router)

        # Register message handlers
        self.router.message.register(self.start_command_handler, Command(commands="start"))
        self.router.message.register(self.help_command_handler, Command(commands="help"))
        self.router.message.register(self.about_command_handler, Command(commands="about"))
        self.router.message.register(
            self.set_birthday_command_handler, Command(commands="set_birthday")
        )
        # Администраторы имеют возможность менять текст поздравления для указанного пользователя
        #   через команду /set_congrat_text [user_id] [текст]
        self.router.message.register(
            self.set_congrat_text_command_handler, Command(commands="set_congrat_text")
        )
        # /delete_birthday — для удаления даты рождения. (для текущего чата)
        #   + (опционально) для указанного пользователя, если пользователь - админ или владелец
        #       бота
        self.router.message.register(
            self.delete_birthday_command_handler, Command(commands="delete_birthday")
        )
        self.router.message.register(
            self.congrat_today_command_handler, Command(commands="congrat_today")
        )
        self.router.message.register(
            self.congrat_text_command_handler, Command(commands="congrat_text")
        )
        # TODO: start_congrats
        # TODO: stop_congrats

        # Initialize the scheduler
        # self.scheduler = AsyncIOScheduler()
        # self.scheduler.add_job(self.scheduled_task, 'interval', seconds=10)  # Run every 10 seconds
        # self.scheduler.start()


    @staticmethod
    def parse_date(date_str: str) -> [bool, datetime.date]:
        try:
            return datetime.strptime(date_str, '%d.%m')
        except ValueError:
            return False

    async def is_admin_or_bot_owner(self, user_id: int, chat_id: int) -> bool:
        # TODO: add docstring
        self.logger.debug('Checking if user %s is an admin or owner', user_id)

        # Check if the user is the main admin (owner)
        if user_id == secrets.MAIN_ADMIN_TG_USER_ID:
            self.logger.debug('User %s is the bot owner', user_id)
            return True

        # Check if the user is an admin in the current context
        try:
            admins = await self.bot.get_chat_administrators(chat_id)
            # noinspection PyTypeChecker
            admin_ids = [admin.user.id for admin in admins]
            if user_id in admin_ids:
                self.logger.debug('User %s is an admin in chat %s', user_id, chat_id)
                return True
        except Exception as e:
            self.logger.error('Failed to fetch admin list: %s', str(e))

        self.logger.debug('User %s is neither an admin nor the bot owner', user_id)
        return False

    def check_is_user_id_valid(self, presumably_user_id: [list, tuple]) -> bool:
        # TODO: add docstring
        self.logger.debug('Checking if user id (%s) is a number', presumably_user_id)
        try:
            int(presumably_user_id)
            self.logger.debug('User id is a number')
            return True
        except ValueError as e:
            # TODO: extra confirmation in that case
            self.logger.debug('First word after command is not a number')
            self.logger.debug(e)

            return False

    async def get_username(self, chat_id: int, user_id: int) -> [str, None]:
        # TODO: add docstring
        try:
            # Fetch ChatMember object
            chat_member = await self.bot.get_chat_member(
                chat_id=chat_id, user_id=user_id
            )
            # Extract username or fallback to first name
            username = chat_member.user.username or chat_member.user.first_name
            return username
        except Exception as e:
            # Handle exceptions (e.g., user not found)
            self.logger.error('Failed to fetch username: %s', str(e))
            return

    async def start_command_handler(self, message: Message):
        # TODO: add docstring
        self.logger.debug('Running /start command')
        # TODO: actualize commands
        text = (
            'commands: /help /about /set_birthday /set_congrat_text /delete_birthday'
        )
        if message.from_user.id == secrets.MAIN_ADMIN_TG_USER_ID:
            self.logger.debug('User is admin or bot owner')
            text += (
                '\n\nBecause you are an admin - you can use '
                '/congrat_today, /start_congrats, /stop_congrats commands 😉'

                '\n\nYou can also set a congratulation message not only for yourself but for any '
                'user as well! Just add user id before the text to do so. Example:\n'
                '/set_congrat_text 1234 Congratulations! \N{PARTY POPPER}'

                '\n\nYou can also delete a birthday not only for yourself but for any user as '
                'well! Just add user id before the text to do so. Example:\n'
                '/delete_birthday 1234'
            )
        await message.answer(text)

    async def help_command_handler(self, message: Message):
        # TODO: add docstring
        self.logger.debug('Running /help command')
        await self.start_command_handler(message)
        await message.answer((
            'Despite that, there is only one person who can help you: '
            + '@' + secrets.MAIN_ADMIN_TG_USERNAME
        ))

    async def about_command_handler(self, message: Message):
        # TODO: add docstring
        self.logger.debug('Running /about command')
        await message.answer((
                'Check out source and suggest an issue: '
                + f'[GITHUB]({constants.GITHUB_URL})' + '\n'
                + 'Ask about this bot: '
                + '@' + secrets.MAIN_ADMIN_TG_USERNAME
        ))

    async def set_birthday_command_handler(self, message: Message):
        # TODO: add docstring
        self.logger.debug('Running /set_birthday command')

        # TODO: ?move empty command check to a method?
        if (
                message.text == '/set_birthday'
                or message.text == f'/set_birthday@{secrets.BOT_NAME}'
        ):
            self.logger.error('Got empty command')
            await message.answer(
                'Please provide the text that you want to get as a congratulation after command'
            )
            return

        split_message_text = message.text.split()

        # TODO: ?remove repeated check?
        if (
                len(split_message_text) > 2
                and await self.is_admin_or_bot_owner(message.from_user.id, message.chat.id)
                and self.check_is_user_id_valid(split_message_text[1])
        ):
            target_user_id = int(split_message_text[1])
            birthday = self.parse_date(split_message_text[2])
            try:
                target_username = await self.get_username(
                    chat_id=message.chat.id, user_id=target_user_id
                )
            except TelegramBadRequest as e:
                await message.answer(
                    f'Sorry, but I can`t get user ({target_user_id}) info. '
                    'Check id and try again.'
                )
                if 'user not found' in str(e):
                    msg = 'The user is not in the chat.'
                    self.logger.error(msg)
                    await message.answer(msg)
                else:
                    # Handle other BadRequest errors
                    self.logger.error(f'An unexpected error occurred: {e}')
                return
        elif (
                len(split_message_text) > 2
                and self.check_is_user_id_valid(split_message_text[1])
        ):
            # if user is not admin nor bot owner but still trying to set someonr else`s birthday
            await message.answer(
                'Ha-ha, clever!. But you can`t do that for other users. Try again without user id.'
            )
            return
        else:
            # if user is not an admin nor a bot owner and there is no user_id as parameter
            target_user_id = message.from_user.id
            # TODO: check if it is needed
            target_username = message.from_user.username or message.from_user.first_name
            birthday = self.parse_date(split_message_text[1])

        # check the date
        self.logger.debug('Parsed birthday: %s', birthday)
        if not birthday:
            await message.answer('Invalid date format. Please try again in format "DD.MM".')
            return

        # TODO: move checks to a method
        self.logger.debug('Checking if user already exists')
        if not self.db_tools.user_exists(target_user_id):
            self.logger.debug('Adding new user')
            self.db_tools.add_user(target_user_id, target_username)
        else:
            self.logger.debug('User already exists')

        self.logger.debug('Checking if user already registered in chat')
        if not self.db_tools.user_registered_in_chat(target_user_id, message.chat.id):
            self.logger.debug('Registering the user to chat')
            self.db_tools.add_user_chat(target_user_id, message.chat.id)
        else:
            self.logger.debug('User already registered in chat')

        self.logger.debug(
            'Adding/updating user birthday in DB: %s %s', target_user_id, birthday
        )
        self.db_tools.add_user_birthday(
            target_user_id, message.chat.id, birthday.day, birthday.month
        )

        await message.answer(
            f'Birthday for user {target_user_id} '
            f'({str(birthday.day).zfill(2)}'
            f'.{str(birthday.month).zfill(2)}) '
            'was saved!\n'
            'p.s. if you want to change default congratulation message, you can use '
            '/set_congrat_text command'
        )

    async def set_congrat_text_command_handler(self, message: Message) -> None:
        # TODO: add docstring
        # TODO: ?remove unprintable characters?
        self.logger.debug('Running /set_congrat_text command')
        self.logger.debug('Full command: %s', message.text)

        if (
                message.text == '/set_congrat_text'
                or message.text == f'/set_congrat_text@{secrets.BOT_NAME}'
        ):
            self.logger.error('Got empty command')
            await message.answer(
                'Please provide the text that you want to get as a congratulation after command'
            )
            return

        split_message_text = message.text.split()

        # slicing used to get all text after command as congratulation text so it should be
        #   possible to use whitespaces in it
        if (
                len(split_message_text) > 2
                and await self.is_admin_or_bot_owner(message.from_user.id, message.chat.id)
                and self.check_is_user_id_valid(split_message_text[1])
        ):
            target_user_id = int(split_message_text[1])
            congratulation_text = ' '.join(split_message_text[2:])
        else:
            # if user is not an admin nor a bot owner
            target_user_id = message.from_user.id
            congratulation_text = ' '.join(split_message_text[1:])

        self.db_tools.add_congratulation(target_user_id, message.chat.id, congratulation_text)
        await message.answer(
            f'Congratulation text for user {target_user_id} was updated to: '
            f'"{congratulation_text}" for chat {message.chat.id}'
        )

    async def delete_birthday_command_handler(self, message: Message):
        # TODO: add docstring
        # TODO: ask for confirmation?
        self.logger.debug('Running /delete_birthday command')

        split_message_text = message.text.split()

        if (
                len(split_message_text) > 1
                and await self.is_admin_or_bot_owner(message.from_user.id, message.chat.id)
                and self.check_is_user_id_valid(split_message_text[1])
        ):
            target_user_id = int(split_message_text[1])
        else:
            target_user_id = message.from_user.id

        self.db_tools.delete_user_birthday(target_user_id, message.chat.id)

        await message.answer(f'Birthday was deleted for user: {target_user_id}')

    async def congrat_text_command_handler(self, message: Message):
        # TODO: add docstring
        self.logger.debug('Running /congrat_text command')

        split_message_text = message.text.split()

        if (
                len(split_message_text) > 1
                and await self.is_admin_or_bot_owner(message.from_user.id, message.chat.id)
                and self.check_is_user_id_valid(split_message_text[1])
        ):
            target_user_id = int(split_message_text[1])
        else:
            target_user_id = message.from_user.id

        congratulation_text = self.db_tools.get_user_congratulation(
            target_user_id, message.chat.id
        )

        if not congratulation_text:
            congratulation_text = self.default_congratulation

        await message.answer(
            f'The congratulation for user {target_user_id} is: "{congratulation_text}"'
        )

    async def congrat_today_command_handler(self, message: Message):
        # TODO: add docstring
        self.logger.debug('Running /check_today command')

        users_with_birthday_today = self.db_tools.get_users_with_birthday(
            date.today().day, date.today().month, message.chat.id
        )
        self.logger.debug(
            'Found %d users with birthdays today', len(users_with_birthday_today)
        )

        if len(users_with_birthday_today) == 0:
            await message.answer('There are no registered birthdays today')
            return

        for user in users_with_birthday_today:
            user_congratulation = self.db_tools.get_user_congratulation(
                user.user_id, message.chat.id
            )

            if not user_congratulation:
                user_congratulation = self.default_congratulation

            self.logger.debug(
                'Sending congratulation to user: %s in chat: %s',
                user.user_id, message.chat.id
            )

            await message.answer(f'@{user.user_name} {user_congratulation}')

    # TODO: start method
    # async def remove_job_if_exists(
    #     self, name: str, context: ContextTypes.DEFAULT_TYPE
    # ) -> bool:
    #     """Remove job with given name. Returns whether job was removed."""
    #     current_jobs = context.job_queue.get_jobs_by_name(name)
    #     self.logger.debug(
    #         'Current jobs: %s',
    #         [job.name for job in context.job_queue.jobs()]
    #     )
    #     if not current_jobs:
    #         return False
    #     for job in current_jobs:
    #         job.schedule_removal()
    #     return True

    # TODO: stop method

    # TODO: ?inline format for registration?
    #   https://docs.aiogram.dev/en/v2.25.1/examples/inline_bot.html

    async def run(self) -> None:
        # TODO: add docstring
        # Start polling
        await self.dp.start_polling(self.bot)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--test_run', help='If it is a test run',
        action='store_true'
    )
    args = parser.parse_args()

    birthday_bot = BirthdayBot(
        token=secrets.API_TOKEN if not args.test_run else secrets.API_TOKEN_TEST,
        logging_level=logging.DEBUG,
        test_run=True if args.test_run is True else False
    )

    asyncio.run(birthday_bot.run())
