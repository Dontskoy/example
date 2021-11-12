# этот код - пример одного из самых трудоёмких, ПОЛЕЗНЫХ и работающих корректно проектов на ноябрь 2021
# все чувствительные данные изменены на "censored"

import vk
import random
from oauth2client.service_account import ServiceAccountCredentials
import apiclient.discovery
import httplib2
import time
import pymysql
from typing import List
from typing import Dict
import datetime
import string
import googleapiclient
from pprint import pprint

host = 'localhost'
user_bd = 'censored'
password = 'censored'
database = 'censored'
charset = 'utf8mb4'

vk_token = 'censored'

CREDENTIALS_FILE = r'censored'  # имя файла с закрытым ключом
spreadsheet_id = 'censored'
users_column_info = dict(название_курса=1,
                         id_курса=2,
                         id_пользователя=3,
                         имя=4,
                         id_vk=5,
                         в_беседе=6,
                         наставник=7,
                         в_друзьях=8,
                         в_группе=9,
                         доп_курс=10,
                         из_команды=11,
                         промокод=12,
                         возврат=13)

VK_API_VERSION = "5.131"
vk_log_token = 'censored'

approve_friendship_message = '''censored'''
request_friendship_message = '''censored'''


def logging(fail: str, send_to_vk=1):
    with open('log.txt', 'a') as log_file:
        log_file.write(f"{time.strftime('%m.%d %X')}\t{fail}\n")
    if send_to_vk:
        api_log.messages.send(peer_id=2000000001,
                              group_id='censored',
                              message=f'Beaver_v2\n{fail}',
                              random_id=random.randint(1, 9223372036854775807))
        global count
        count = 0


def find_course_users() -> List[dict]:
    """
    Запрашивает пользователей с сайта
    :return:
        [{id: , user_id: , full_name: ,}]
    """
    cur.execute(f"""SELECT user.id, user.user_id, user.full_name, promocode.name AS promocode
    FROM user_course JOIN user ON user.id=user_course.user_id LEFT JOIN promocode ON promocode.id=user_course.promocode_id
    WHERE user_course.status='STATUS_COMPLETE' AND user_course.course_id={course_id}""")
    return cur.fetchall()


def sent_to_table(sent_data: List[Dict[str, List[list]]], sheet='BASE', majorDimension="ROWS"):
    """
    записывание данных в таблицу
    :param sent_data: список словарей с ключамми "range" и "data"
    :param sheet: название листа. По умолчанию BASE
    :param majorDimension: порядок заполнения данных. По умолчанию ROWS
    :return:
    """
    sent = []
    for sd in sent_data:
        sent.append(
            [{"range": f'{sheet}!{sd["range"]}',
              "majorDimension": majorDimension,
              # сначала заполнять ряды, затем столбцы (т.е. самые внутренние списки в values - это ряды)
              "values": sd["data"]
              }]
        )
    try:
        service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body={
            "valueInputOption": "USER_ENTERED",
            "data": sent
        }).execute()
    except googleapiclient.errors.HttpError as error:
        logging(f"не получилось добавить новые данные в лист \n{error}")
        time.sleep(30)
    time.sleep(1)


def int_to_a1(row: int, column: int) -> str:
    """
    перевод математического адреса ячейки в А1 нотацию
    :param row: строка
    :param column: столбец
    :return:
    """
    column26 = []
    while column > 26:
        column26.append(((column - 1) // 26))
        column = column - 26 * (column // 26)
    column26.append(column)
    output_string = ''
    for char in column26:
        output_string += string.ascii_uppercase[char - 1]
    output_string += str(row)
    return output_string


def find_write_users() -> List[dict]:
    """
    Запрашивает пользователей из таблицы
    :return: [dict(/users_column_info/) for i in sheets_data[1:]]
    """
    sheets_data = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, majorDimension='ROWS',
                                                      range="users!A:N").execute()['values']
    out = []
    for i in sheets_data[1:]:
        temp_dict = {}
        for k in users_column_info:
            temp_dict[k] = i[users_column_info[k] - 1]
        out.append(temp_dict)
    return out


def update_course():
    """
    функция полной работы с пользователями
    :return:
    """
    update_not_found_users()
    try:
        write_new_users()
    except Exception as Error:
        logging(f'Проблемы с загрузкой новый учеников\n\n{Error}')
        time.sleep(5 * 60)
    update_old_users()


def update_not_found_users():
    """
    функция обработки пользователей, подозрительных на возврат
    :return:
    """
    write_data = []
    users_ids = [i['user_id'] for i in course_users]
    for num, i in enumerate(write_users):
        if i['id_курса'] == course_id and int(i['id_vk']) not in users_ids and i['возврат'] not in ['?', '!']:
            write_data.append({'data': [['?']],
                               'range': int_to_a1(row=num + 2, column=users_column_info['возврат'])})
    sent_to_table(write_data, sheet='users')


def write_new_users():
    """
    вписывает новые строки пользователей
    :return:
    """
    write_data = []
    sheet_data = [[i['id_курса'], i['id_пользователя']] for i in write_users]
    free_row = len(write_users) + 2
    for course_user in course_users:
        if [course_id, str(course_user['id'])] not in sheet_data:
            write_data.append({
                'data': [[course_id,
                          course_user['id'],
                          f'=ГИПЕРССЫЛКА("vk.com/id{course_user["user_id"]}";"{course_user["full_name"]}")',
                          course_user["user_id"],
                          '',
                          f'=ЕСЛИ(F{free_row}="";""; ВПР(ПСТР(F{free_row};3;10);info!E:F;2;ЛОЖЬ))',
                          '', '', '',
                          f'=ЕСЛИ(СЧЁТЕСЛИ(info!K:K;E{free_row})=0;"";"+")',
                          course_user["promocode"],
                          '-']],
                'range': f'B{free_row}:N{free_row}'})
            free_row += 1
    sent_to_table(write_data, sheet='users')


def update_old_users():
    """
    проходит по старым пользователям выбранного курса
    :return:
    """
    write_data = []
    user_rows = [i for i in write_users if i['id_курса'] == course_id and i['возврат'] == '-' and i['из_команды'] == '']
    if to_friends == 'TRUE':
        a_t_f_list = append_to_friends(all_data=write_users, user_rows=user_rows)
        if a_t_f_list:
            write_data += a_t_f_list
    if dop_course_id != '':
        try:
            g_d_c_list = give_dop_course(all_data=write_users, user_rows=user_rows)
            if g_d_c_list:
                write_data += g_d_c_list
        except Exception as dop_course_error:
            logging(f'Проблемы с выдачей дополнительного курса\n{dop_course_error}')
    if conversation_ids != '':
        try:
            a_t_c_str = conversation_function(all_data=write_users, user_rows=user_rows)
            if a_t_c_str:
                write_data += a_t_c_str
        except Exception as conversation_error:
            logging(f'Проблемы с распределением по беседам\n{conversation_error}')
    if group_ids != '':
        a_t_g_str = append_to_group(all_data=write_users, user_rows=user_rows)
        if a_t_g_str:
            write_data += a_t_g_str
    sent_to_table(write_data, sheet='users')


def append_to_group(all_data: List[dict], user_rows: List[dict]) -> List[dict]:
    """
    для каждой группы, в которую нужно добавить, находим тех, кто ещё не добавлен,
    добавляем тех, кто постучался в группу
    смотрим, кто в группе и обновляем таблицу
    приглашаем оставшихся
    :return:
    """
    out = []
    groups = group_ids.split(', ')
    for group in groups:
        need_to_append_users = [i for i in user_rows if group not in i['в_группе']]
        if not need_to_append_users:
            continue
        approve_requests_to_the_group(group=group, users=[i['id_vk'] for i in user_rows])
        group_users = find_group_users(group=group)
        # записываем тех кто в группе
        for user in user_rows:
            if int(user['id_vk']) in group_users and f'+ {group}' not in user['в_группе']:
                if group in user['в_группе']:
                    status = user['в_группе'].replace(f'e {group}', f'+ {group}').replace(f'->{group}',
                                                                                          f'+ {group}').replace(
                        f'x {group}', f'+ {group}')
                else:
                    status = f'+ {group}' if user['в_группе'] == '' else f'{user["в_группе"]}, + {group}'
                out += update_users_status(rows=[all_data.index(user) + 2], column=users_column_info['в_группе'],
                                           status=f'="{status}"')
        # список приглашённых:
        invited_group_users = find_invited_group_users(group=group)
        # приглашаем тех, кого нет в группе
        for user in [i for i in need_to_append_users if int(i['id_vk']) not in group_users and i['в_друзьях'] == '+']:
            try:
                if int(user['id_vk']) not in invited_group_users:
                    vk_api.groups.invite(group_id=group, user_id=user['id_vk'])
                status = "->"
            except vk.exceptions.VkAPIError as error:
                mentor_data = search_a_mentor_by_conversation(user['в_беседе'][2:])
                mentor_name, mentor_id = (mentor_data.get('vk_name', 'no_name'), mentor_data.get('vk_id', 0))
                status = except_vk_error(error=str(error),
                                         start_string=f'Не получилось пригласить @id{user["id_vk"]}({user["имя"]}) ' +
                                                      f'в группу @club{group}.\n@id{mentor_id}({mentor_name})\n')
            status = f'{status}' if user['в_группе'] == '' else f"{user['в_группе']}, {status}"
            out.append({
                'range': int_to_a1(row=all_data.index(user) + 2, column=users_column_info['в_группе']),
                'data': [[f'="{status}{group}"']]
            })
    return out


def find_group_users(group: str) -> list:
    group_vk_data = vk_api.groups.getMembers(group_id=group, sort='id_asc', count=1000)
    out = group_vk_data['items']
    for i in range(group_vk_data['count'] // 1000):
        out += vk_api.groups.getMembers(group_id=group, offset=i * 1000, sort='id_asc', count=1000)['items']
    return out


def find_invited_group_users(group: str) -> list:
    group_vk_data = vk_api.groups.getInvitedUsers(group_id=group, sort='id_asc')
    out = [i['id'] for i in group_vk_data['items']]
    for i in range(group_vk_data['count'] // 20):
        out += [i['id'] for i in vk_api.groups.getInvitedUsers(group_id=group, offset=i * 20, sort='id_asc')['items']]
    return out


def approve_requests_to_the_group(group: str, users: list):
    request_users = vk_api.groups.getRequests(group_id=group, count=200).get('items')
    for user in request_users:
        if str(user) in users:
            vk_api.groups.approveRequest(group_id=group, user_id=user)


def conversation_function(all_data: List[dict], user_rows: List[dict]) -> List[dict]:
    """
    Выбираем, кто ещё нераспределён. Смотрим численность групп вк.
    Проверяем, кто распределился самостоятельно и обновляем его статус.
    Распределяем (вносим правки) и приглашаем оставшихся
    :param all_data:
    :param user_rows:
    :return: возвращает список словарей с ключами range и data
    """
    out = []
    # данные из таблицы, кого надо добавить
    need_to_append_users = [i for i in user_rows if '+' not in i['в_беседе']]
    if not need_to_append_users:
        return out
    conversations = conversation_ids.split(', ')
    if conversation_limit == '':
        limit = 999999999
    else:
        limit = int(conversation_limit)
    # данные по людям в беседах из вк
    conversations_data = find_vk_conversations(conversations=conversations)
    # создаём список бесед
    conversations_write_data = {}
    for i in conversations:
        conversations_write_data[i] = [u for u in user_rows if i in u['в_беседе']]
    # обновляем данные по тем, кто уже в беседе
    for u in need_to_append_users:
        for k, v in conversations_data.items():
            if int(u['id_vk']) in v:
                out.append({'range': int_to_a1(row=all_data.index(u) + 2, column=users_column_info['в_беседе']),
                            'data': [[f'="+ {k}"']]})
                conversations_write_data[k].append(u['id_vk'])
                break
    append_user = []
    for i in conversations_write_data:
        append_user += conversations_write_data[i]
    # распределяем людей по беседам и обновляем данные в таблице
    need_to_distribute_users = [i for i in user_rows if (i['в_беседе'] == '' or '? ' in i['в_беседе'])
                                and i['id_vk'] not in append_user]
    for u in need_to_distribute_users:
        min_conv = min(conversations_write_data, key=lambda x: len(conversations_write_data[x]))
        if len(conversations_write_data[min_conv]) >= limit:
            logging(f'Закончилось место в беседах {conversation_ids}')
            return out
        if u['в_друзьях'] == '+':
            try:
                min_conv = min_conv if u['в_беседе'] == '' else u['в_беседе'][2:]
                vk_api.messages.addChatUser(chat_id=min_conv, user_id=u['id_vk'], visible_messages_count=1000)
                status = "->"
            except vk.exceptions.VkAPIError as error:
                mentor_data = search_a_mentor_by_conversation(min_conv)
                temp_n, temp_id = (mentor_data.get('vk_name', 'no_name'), mentor_data.get('vk_id', 0))
                status = except_vk_error(error=str(error),
                                         start_string=f'Не получилось пригласить в беседу №{min_conv} @id{u["id_vk"]}' +
                                                      f'({u["имя"]}) к наставнику @id{temp_id}({temp_n}). ')
        elif u['в_беседе'] == '':
            status = '? '
        else:
            continue
        out.append({
            'range': int_to_a1(row=all_data.index(u) + 2, column=users_column_info['в_беседе']),
            'data': [[f'="{status}{min_conv}"']]
        })
        conversations_write_data[min_conv].append(u['id_vk'])
    return out


def search_a_mentor_by_conversation(conf: str) -> dict:
    """
    поиск наставника по номеру конфы. Дынные берутся из листа info
    :param conf:
    :return: возвращает наставника (словарь) с заданной группой, если она есть. Иначе пустой словарь
    """
    mentor_data = [i for i in assistants_vk_id if i['conf'] == conf]
    out = mentor_data[0] if len(mentor_data) > 0 else {}
    return out


def except_vk_error(error: str, start_string='') -> str:
    status = "x "
    report = start_string
    time_sleep = 60
    if error[:3] == "939":
        status = "->"
        report += "Приглошение уже отправленно"
    elif error[:3] == "15.":
        status = "e "
        report += "Доступ запрещён. "
        if 'already sent' in error:
            report += 'Уже приглашал'
        elif 'could invite only friends' in error:
            report += 'Могут приглашать только друзья'
        else:
            report += f"\n{error}"
        time_sleep = 10
    elif error[:3] == "14.":
        report += "Необходимо ввести капчу"
        time_sleep = 5 * 60
    elif error[:3] == "9. ":
        report += "Слишком много однотипных действий (флуд)"
        time_sleep = 5 * 60
    else:
        report += f"\n{error}"
    logging(report)
    time.sleep(time_sleep)
    return status


def find_vk_conversations(conversations: List[str]) -> Dict[str, List[int]]:
    """
    запрашивает даннные о членах беседы
    :param conversations: список строк с номерами бесед
    :return: возвращает словарь с номерами бесед в качестве ключей и списками id-шников в качестве значений
    """
    out = {}
    for conversation in conversations:
        try:
            conversation_data = vk_api.messages.getConversationMembers(peer_id=2000000000 + int(conversation))
            out[conversation] = [i['member_id'] for i in conversation_data['items'] if i.get('join_date') is not None]
        except Exception as vk_api_conv_error:
            logging(f'Не удалось найти участников беседы\n{vk_api_conv_error}')
    return out


def append_to_friends(all_data: List[dict], user_rows: List[dict]) -> List[dict]:
    """
    пробует добавить в друзья всех, кто ещё не добавлен
    тем, кто принял заявку отправляет приветственное сообщение
    :param all_data: все строчки в листе users
    :param user_rows: строки пользователей, отноящиеся только к выбранному курсу
    :return: список словарей с ключами "data" и "range"
    """
    out = []
    # список тех, кто не в друзьх по таблице (дальше работаем с ним)
    not_friends_user = [i for i in user_rows if i['в_друзьях'] != '+']
    if not not_friends_user:
        return out
    try:
        are_friends_response = vk_api.friends.areFriends(user_ids=[i['id_vk'] for i in not_friends_user])
    except vk.exceptions.VkAPIError as error:
        except_vk_error(error=str(error), start_string='Не получилось проверить статус дружбы. ')
        return out
    #  приём в друзья тех, кто сам постучал
    add_friends(users=[i['user_id'] for i in are_friends_response if i['friend_status'] == 2],
                message=approve_friendship_message)
    #  обновление статуса у тех, кто уже в друзьях
    are_friends_now = [i['user_id'] for i in are_friends_response if i['friend_status'] == 3]
    out += update_users_status(
        rows=[num + 2 for num, v in enumerate(all_data) if int(v['id_vk']) in are_friends_now],
        column=users_column_info['в_друзьях'], status='="+"')
    #  запрос в друзья новых людей
    not_friends_vk = [i['user_id'] for i in are_friends_response if i['friend_status'] == 0]
    failed_user = add_friends(users=list({u['id_vk'] for u in all_data if int(u['id_vk']) in not_friends_vk
                                          and '! ' not in u['в_друзьях']}),
                              message=request_friendship_message)
    out += update_users_status(
        rows=[num + 2 for num, v in enumerate(all_data) if int(v['id_vk']) in failed_user
              and '! ' not in v['в_друзьях'] and '->' not in v['в_друзьях']],
        column=users_column_info['в_друзьях'], status=f'="! "')
    #  обновление статуса у тех, к кому постучали в друзья
    new_request = [i['user_id'] for i in are_friends_response if i['friend_status'] == 1]
    # люди не в друзьях, которым ещё не шло уведомление
    out += update_users_status(
        rows=[num + 2 for num, v in enumerate(all_data) if int(v['id_vk']) in new_request
              and '! ' not in v['в_друзьях'] and '->' not in v['в_друзьях']],
        column=users_column_info['в_друзьях'], status=f'="->{time.time()}"')

    # новая фишка с отсчётом времени от запроса в друзья
    report = ''
    for num, u in enumerate(all_data):
        if u['id_курса'] == course_id \
                and int(u['id_vk']) in new_request and '! ' not in u['в_друзьях'] and u['в_друзьях'] != '':
            if time.time() - float(u['в_друзьях'][2:]) > 60 * 10:
                mentor_data = search_a_mentor_by_conversation(u['в_беседе'][2:])
                mentor_name, mentor_id = (mentor_data.get('vk_name', 'no_name'), mentor_data.get('vk_id', 0))
                out += update_users_status(rows=[num + 2], column=users_column_info['в_друзьях'],
                                           status=f'''="! {u['в_друзьях'][2:]}"''')
                report += f'''@id{u['id_vk']}({u['имя']}) не добавляет в друзья @id611573472(Петьку) у наставника @id{mentor_id}({mentor_name})\n'''
    if report != '':
        logging(report)
    return out


def add_friends(users: List[int], message='') -> List[int]:
    """
    возвращает список id которых не получилось добавить в друзья
    :param users:
    :param message:
    :return:
    """
    out = []
    for u in users:
        try:
            vk_api.friends.add(user_id=u, text=message)
            time.sleep(15)
        except vk.exceptions.VkAPIError as error:
            except_vk_error(error=str(error), start_string='Не получилось добавить в друзья. ')
            out.append(u)
    return out


def update_users_status(rows: List[int], column: int, status: str) -> List[dict]:
    out = [dict(data=[[status]], range=int_to_a1(row=i, column=column)) for i in rows]
    return out


def give_dop_course(all_data: List[dict], user_rows: List[dict]) -> List[dict]:
    """
    раздача доп курсов.
    :return:
    """
    out = []
    users_in_need = [i for i in user_rows if dop_course_id not in i['доп_курс'] and i['возврат'] != '!']
    if not users_in_need:
        return out
    # переписать выбор!
    cur.execute(
        f'''SELECT id FROM user WHERE id IN {tuple([i['id_пользователя'] for i in users_in_need] + [-1, -2])} AND
        id NOT IN (
        SELECT user_id FROM user_course WHERE status='STATUS_COMPLETE' AND course_id={dop_course_id})''')
    users_3w_id = cur.fetchall()
    for user_3w_id in users_3w_id:
        user_id = user_3w_id['id']
        date = datetime.datetime.today()
        cur.execute(
            f'''INSERT user_course (user_id, date, course_id, status, tarif, created_by_id, promocode_id) 
            VALUES ({user_id}, "{date}", {dop_course_id}, "STATUS_COMPLETE", "TARIFF_SILVER", 4276, 614)''')
        con.commit()
    for user_in_need in users_in_need:
        out.append(dict(data=[[(user_in_need['доп_курс'] + f', {dop_course_id}').lstrip(', ')]],
                        range=int_to_a1(row=all_data.index(user_in_need) + 2, column=users_column_info['доп_курс'])))
    return out


def read_settings() -> List[List[str]]:
    """
    возвращает настройки раздачи курсов из
    https://docs.google.com/spreadsheets/d/censored
    :return:
    """
    sheets_data = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, majorDimension='ROWS',
                                                      range="settings!A:J").execute()['values'][1:]
    return sheets_data


def rebut_assistant_users_list():
    """
    читаем список групп,
    добавляем новых, удаляем лишних,
    зачищаем лишнее
    :return:
    """
    for assistant_vk_id in assistants_vk_id:
        # проверка наличия аккаунта у наставника
        if cur.execute(f"SELECT id FROM user WHERE user_id={assistant_vk_id.get('vk_id')}") == 0:
            logging(f"Нужно зайти на сайт со страницы @id{assistant_vk_id.get('vk_id')}")
            continue
        t = cur.execute(f'''SELECT assistent_user.id 
        FROM assistent_user JOIN user ON user.id=assistent_user.assistent_new_id
        WHERE user.user_id={assistant_vk_id.get('vk_id')} AND assistent_user.course_id={course_id} 
        AND assistent_user.name="{assistant_vk_id.get('name', "").replace('"', '""')}"''')
        assistant_id_data = cur.fetchone()
        # если группы нет, создаём ей и пишем, что нужно поменять название
        if t < 1:
            assistant_id = make_new_assistant(assistant_vk_id=assistant_vk_id.get('vk_id'), name=assistant_vk_id.get('name').replace('"', '""'))
            logging(f"@id{assistant_vk_id.get('vk_id')}, создал для тебя новую группу на сайте")
        # если группа есть, удаляем всех её представителей
        else:
            assistant_id = assistant_id_data.get('id')
            delete_assist_users(assistant_id=assistant_id)
        users_list = [i['id_пользователя'] for i in write_users if
                      i['в_беседе'][2:] == assistant_vk_id['conf'] and i['возврат'] != '!' and i['из_команды'] != '+'
                      and i['id_курса'] == course_id and i['промокод'] != 'gift_from_bot']
        if users_list:
            append_user_to_assist(users=users_list, assistant_id=assistant_id)
    sent_to_table(sent_data=[{'data': [['FALSE']], 'range': f"G{settings.index(course_settings) + 2}"}],
                  sheet='settings')


def make_new_assistant(assistant_vk_id: str, name: str) -> int:
    cur.execute(f'''SELECT id FROM user WHERE user_id={assistant_vk_id}''')
    user_id = cur.fetchone()
    cur.execute(
        f'''INSERT assistent_user (assistent_new_id, name, course_id) VALUES 
        ({user_id['id']}, "{name}", {course_id})''')
    con.commit()
    cur.execute(f'''SELECT id FROM assistent_user WHERE assistent_new_id={user_id['id']} AND course_id={course_id} 
    AND name={name}''')
    return cur.fetchone()['id']


def delete_assist_users(assistant_id: int):
    """
    чистит группу у наставника
    :param assistant_id:
    :return:
    """
    cur.execute(f'''DELETE FROM assistent_user_user WHERE assistent_user_id={assistant_id}''')
    con.commit()


def append_user_to_assist(users: List[str], assistant_id: int):
    request = 'INSERT assistent_user_user (assistent_user_id, user_id) VALUES '
    request += ', '.join([f'({assistant_id}, {u})' for u in users])
    cur.execute(request)
    con.commit()


def parsing_input_data(input_data):
    output_data = []
    # добавляем номер курса
    if len(input_data) > 2:
        output_data.append(input_data[2])
    else:
        output_data.append('')
    # добавляем текст
    if len(input_data) > 4:
        output_data.append(input_data[4])
    else:
        output_data.append('')
    # добавляем фото
    if len(input_data) > 5:
        output_data.append(input_data[5])
    else:
        output_data.append('')
    # добавляем фото
    if len(input_data) > 6:
        output_data.append(input_data[6])
    else:
        output_data.append('')
    # добавляем фото
    if len(input_data) > 7:
        output_data.append(input_data[7])
    else:
        output_data.append('')
    # добавляем фильтр
    if len(input_data) > 8:
        output_data.append(input_data[8])
    else:
        output_data.append('')
    # добавляем флаг
    if len(input_data) > 11:
        output_data.append(input_data[11])
    else:
        output_data.append('')
    return tuple(output_data)


if __name__ == '__main__':
    count = 0
    while True:
        try:
            session_log = vk.Session(access_token=vk_log_token)
            api_log = vk.API(session_log, v=VK_API_VERSION)
            credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE,
                                                                           [
                                                                               'https://www.googleapis.com/auth/spreadsheets',
                                                                               'https://www.googleapis.com/auth/drive'])
            httpAuth = credentials.authorize(httplib2.Http())
            service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

            con = pymysql.connect(host=host, user=user_bd, password=password, database=database, charset=charset,
                                  cursorclass=pymysql.cursors.DictCursor)
            cur = con.cursor()

            vk_session = vk.Session(access_token=vk_token)
            vk_api = vk.API(vk_session, v="5.131")
            settings = read_settings()
            assistant_data = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, majorDimension='ROWS',
                                                                 range="info!E:H").execute()['values'][1:]
            for course_settings in settings:
                setting_length = len(course_settings)
                # интерпритируем строку настроек
                try:
                    [course_name,
                     course_id,
                     dop_course_name,
                     dop_course_id,
                     conversation_ids,
                     conversation_limit,
                     rebut_web_list,
                     group_ids,
                     to_friends] = course_settings + [''] * (9 - setting_length)
                    if course_id == '':
                        continue
                except Exception as settings_error:
                    logging(f'Проблемы с прочтением настроек курса:\n{course_settings}\n{settings_error}')
                    continue
                assistants_vk_id = [{'conf': i[0], 'vk_name': i[1], 'vk_id': i[2], 'name': i[3]} for i in assistant_data if
                                    i[0] in conversation_ids and i[2] != '']
                # запрашиваем пользователей из БД
                try:
                    course_users = find_course_users()
                except Exception as course_users_error:
                    logging(f'Проблемы с нахождением пользователей курса {course_id}\n{course_users_error}')
                    continue
                # запрашиваем пользователей из таблицы
                try:
                    write_users = find_write_users()
                except Exception as write_users_error:
                    logging(f'Проблемы с нахождением вписанных юзеров\n{write_users_error}')
                    continue
                if rebut_web_list == "TRUE" and conversation_ids != '':
                    try:
                        rebut_assistant_users_list()
                    except Exception as rebut_assistant_error:
                        logging(f'Проблемы с обновлением списка группы на сайте\n{rebut_assistant_error}')
                update_course()
            time.sleep(60)
        except BaseException as FatalError:
            logging(f'Непредвиденные проблемы\n\n{FatalError}', send_to_vk=None)
            time.sleep(5 * 60)
        count += 1
        if count == 360:
            logging('я живой')
            count = 0
