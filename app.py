import pandas as pd
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher, FSMContext
from aiogram.dispatcher.filters import Command
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
import aiocron
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

f = open('token', 'r')
token = f.read()
f.close()

bot = Bot(token=token)
dp = Dispatcher(bot, storage=MemoryStorage())


df = pd.read_csv('BD.csv')
pairs = pd.DataFrame(columns=['user1', 'user2'])


request_priority = {
    'Ищу инвестиции': ['Ищу проекты для инвестиций', 'Ищу партнеров для проекта', 'Ищу интересного и полезного общения в рамках обсуждения сферы деятельности', 'Ищу клиентов', 'Ищу экспертов или консультантов'],
    'Ищу место работы': ['Ищу сотрудников', 'Ищу экспертов или консультантов', 'Ищу интересного и полезного общения в рамках обсуждения сферы деятельности', 'Ищу партнеров для проекта'],
    'Ищу интересного и полезного общения в рамках обсуждения сферы деятельности': ['Ищу интересного и полезного общения в рамках обсуждения сферы деятельности', 'Ищу инвестиции', 'Ищу место работы', 'Ищу сотрудников', 'Ищу клиентов', 'Ищу экспертов или консультантов', 'Ищу партнеров для проекта', 'Ищу проекты для инвестиций'],
    'Ищу сотрудников': ['Ищу место работы', 'Ищу экспертов или консультантов', 'Ищу интересного и полезного общения в рамках обсуждения сферы деятельности', 'Ищу партнеров для проекта'],
    'Ищу клиентов': ['Ищу инвестиции', 'Ищу место работы', 'Ищу сотрудников', 'Ищу интересного и полезного общения в рамках обсуждения сферы деятельности', 'Ищу экспертов или консультантов', 'Ищу партнеров для проекта', 'Ищу проекты для инвестиций'],
    'Ищу экспертов или консультантов': ['Ищу место работы', 'Ищу проекты для инвестиций', 'Ищу интересного и полезного общения в рамках обсуждения сферы деятельности', 'Ищу партнеров для проекта'],
    'Ищу партнеров для проекта': ['Ищу партнеров для проекта', 'Ищу проекты для инвестиций',  'Ищу интересного и полезного общения в рамках обсуждения сферы деятельности',  'Ищу клиентов',  'Ищу экспертов или консультантов'],
    'Ищу проекты для инвестиций': ['Ищу инвестиции',' Ищу экспертов или консультантов',' Ищу интересного и полезного общения в рамках обсуждения сферы деятельности',' Ищу партнеров для проекта']
}

type_priority = {
    "Основатель стартапа": ["Инвестор", "Представитель компании", "Владелец продукта", "Специалист"],
    "Владелец продукта": ["Инвестор", "Представитель компании", "Основатель стартапа", "Специалист"],
    "Представитель компании": ["Основатель стартапа", "Владелец продукта", "Специалист"],
    "Специалист": ["Основатель стартапа", "Владелец продукта", "Представитель компании"],
    "Инвестор": ["Основатель стартапа", "Владелец продукта", "Представитель компании", "Специалист"]
}


class Form(StatesGroup):
    name = State()
    field = State()
    type = State()
    skills = State()
    about = State()
    request = State()


@dp.message_handler(commands='start')
async def start_command(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    button = types.KeyboardButton('Вперед')
    keyboard.add(button)
    await bot.send_message(message.from_user.id, 'Привет! Я IT Match Bot.', reply_markup=keyboard)


@dp.message_handler(lambda message: message.text == 'Вперед')
async def ask_name(message: types.Message):
    await Form.name.set()
    await bot.send_message(message.from_user.id, 'Пожалуйста, введите ваше имя.',
                           reply_markup=types.ReplyKeyboardRemove())


@dp.message_handler(state=Form.name)
async def get_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await Form.next()
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ['GameDev', 'EdTech', 'Crypto', 'FinTech', 'AI', 'WebDev']
    keyboard.add(*buttons)
    await bot.send_message(message.from_user.id, 'Выберите вашу сферу деятельности.', reply_markup=keyboard)


@dp.message_handler(lambda message: message.text in ['GameDev', 'EdTech', 'Crypto', 'FinTech', 'AI', 'WebDev'],
                    state=Form.field)
async def get_field(message: types.Message, state: FSMContext):
    await state.update_data(field=message.text)
    await Form.next()
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ['Основатель стартапа', 'Владелец продукта', 'Инвестор', 'Представитель компании', 'Специалист']
    keyboard.add(*buttons)
    await bot.send_message(message.from_user.id, 'Выберите ваш род деятельности.', reply_markup=keyboard)


@dp.message_handler(
    lambda message: message.text in ['Основатель стартапа', 'Владелец продукта', 'Инвестор', 'Представитель компании',
                                     'Специалист'], state=Form.type)
async def get_type(message: types.Message, state: FSMContext):
    await state.update_data(type=message.text)
    await Form.next()
    await bot.send_message(message.from_user.id,
                           'Расскажите о своих навыках и ресурсах. Чем вы могли бы быть полезны собеседнику? '
                           'Постарайтесь уложиться в рамках 500 символов :)',
                           reply_markup=types.ReplyKeyboardRemove())


@dp.message_handler(state=Form.skills)
async def get_skills(message: types.Message, state: FSMContext):
    skills = message.text[:500]  # обрезаем ввод пользователя до 500 символов
    await state.update_data(skills=skills)
    await Form.next()
    await bot.send_message(message.from_user.id, 'Расскажите собеседнику о себе, о ваших интересах и увлечениях. Так '
                                                 'вам будет легче начать знакомство :)')


@dp.message_handler(state=Form.about)
async def get_about(message: types.Message, state: FSMContext):
    about = message.text[:800]  # обрезаем ввод пользователя до 800 символов
    await state.update_data(about=about)
    await Form.next()
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ['Ищу инвестиции', 'Ищу место работы', 'Ищу интересного и полезного общения в рамках обсуждения сферы '
                                                     'деятельности', 'Ищу сотрудников', 'Ищу клиентов',
               'Ищу экспертов или консультантов', 'Ищу партнеров для проекта', 'Ищу проекты для инвестиций']
    keyboard.add(*buttons)
    await bot.send_message(message.from_user.id, 'Выберете самый подходящий для вас запрос. Запрос подразумевает ваши '
                                                 'ожидания от знакомства.', reply_markup=keyboard)


@dp.message_handler(lambda message: message.text in ['Ищу инвестиции', 'Ищу место работы', 'Ищу интересного и '
                                                                                           'полезного общения в '
                                                                                           'рамках обсуждения сферы '
                                                                                           'деятельности',
                                                     'Ищу сотрудников', 'Ищу клиентов', 'Ищу экспертов или '
                                                                                        'консультантов',
                                                     'Ищу партнеров для проекта', 'Ищу проекты для инвестиций'],
                    state=Form.request)
async def get_request(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    name = user_data.get('name')
    request = message.text
    await state.update_data(request=request)

    df = pd.read_csv('BD.csv')
    user_id = message.from_user.id
    if user_id in df['ID'].values:
        df.loc[df['ID'] == user_id, ['username', 'name', 'field', 'type', 'skills', 'about', 'request']] = [
            message.from_user.username, user_data['name'], user_data['field'], user_data['type'], user_data['skills'],
            user_data['about'], request]
    else:
        df.loc[len(df)] = [user_id, message.from_user.username, user_data['name'], user_data['field'],
                           user_data['type'], user_data['skills'], user_data['about'], request]
    print(message.from_user.username)
    df.to_csv('BD.csv', index = False)
    await state.finish()
    await bot.send_message(message.from_user.id, f'Спасибо за информацию, {name}!',
                           reply_markup=types.ReplyKeyboardRemove())


async def send_unique_message(user_id):
    if user_id < 1000:
        # Это тестовый пользователь, не отправляем ему сообщение
        return

    df = pd.read_csv('BD.csv')

    pair = pairs[(pairs['user1'] == user_id) | (pairs['user2'] == user_id)]
    if pair.empty:
        user_data = df[df['ID'] == user_id].iloc[0].to_dict()
        other_users = df[(df['ID'] != user_id) & (df['field'] == user_data['field'])]

        for request in request_priority[user_data['request']]:
            potential_pairs = other_users[other_users['request'] == request]
            if not potential_pairs.empty:
                break

        for type in type_priority[user_data['type']]:
            potential_pairs = potential_pairs[potential_pairs['type'] == type]
            if not potential_pairs.empty:
                break

        if not potential_pairs.empty:
            other_user_id = potential_pairs.sample(n=1)['ID'].values[0]
        else:
            unpaired_users = df[~df['ID'].isin(pairs.values.flatten())]
            if not unpaired_users.empty:
                other_user_id = unpaired_users.sample(n=1)['ID'].values[0]
            else:
                await bot.send_message(user_id, "К сожалению, мы не смогли найти подходящую пару для вас на данный момент.")
                return

        pairs.loc[len(pairs)] = [user_id, other_user_id]
    else:
        other_user_id = pair['user1'].values[0] if pair['user2'].values[0] == user_id else pair['user2'].values[0]

    other_user_data = df[df['ID'] == other_user_id].iloc[0].to_dict()
    message = f"Привет! Вот данные другого пользователя: {other_user_data}"
    await bot.send_message(user_id, message)




async def send_messages():
    # Здесь вы можете получить список всех пользователей, которым нужно отправить сообщение.
    # Это может быть список идентификаторов чата из вашей базы данных.
    df = pd.read_csv('BD.csv')
    user_ids = df['ID'].tolist()
    for user_id in user_ids:
        asyncio.create_task(send_unique_message(user_id))

scheduler = AsyncIOScheduler()
scheduler.add_job(send_messages, 'interval', minutes=10, start_date='2023-10-1 16:30:00')
scheduler.start()


if __name__ == '__main__':
    from aiogram import executor

    executor.start_polling(dp)