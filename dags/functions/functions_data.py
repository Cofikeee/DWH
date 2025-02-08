from datetime import datetime


def fix_datetime(timestamp):
    if timestamp:
        if not timestamp == '-':
            dt = datetime.strptime(timestamp, '%a, %d %b %Y %H:%M:%S %z')
            return dt.replace(tzinfo=None)
        return None
    return None


def fix_bool(response):
    if response:
        if not response == '':
            return True
        return False
    return False


def fix_null(response):
    if response:
        if not response == '':
            return response.strip()
        return None
    return None


def fix_int(response):
    if type(response) is str:
        return int(response) if response else None
    elif type(response) is list:
        int_array = []
        for r in response:
            int_array += [int(r)]
        return int_array

# ----------------------------------------------------------------


def get_user_roles_array(user):
    roles = {
        'cf_7012': 'Администратор',
        'cf_7011': 'Кадровик',
        'cf_7010': 'Руководитель',
        'cf_7013': 'Делопроизводитель',
    }
    roles_array = []
    for cf_key, value in roles.items():
        if user.get('custom_fields', {}).get(cf_key):
            roles_array += [value]
    return roles_array  # if len(roles_array) > 0 else None


def fix_channel(response):
    channel_mapping = {
        'phone': 'user_phone',
        'telegram': 'telegram_id',
        'email': 'user_email',
        'vkontakte': 'vkontakte_id',
        'facebook': 'facebook_id',
        'wa': 'wa_id',
        'mattermost': 'mattermost_id'
    }
    return channel_mapping.get(response, response)

